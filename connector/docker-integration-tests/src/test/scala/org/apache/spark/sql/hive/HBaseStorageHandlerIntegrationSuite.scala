/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.File
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.BuildImageResultCallback
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model.{ExposedPort, Frame, HostConfig, PortBinding}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Get, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.jdbc.DockerIntegrationFunSuite
import org.apache.spark.tags.DockerTest
import org.apache.spark.util.Utils

/**
 * Spark reads and writes a Hive table STORED BY HBaseStorageHandler. HBase runs standalone in
 * Docker (the image is built from src/test/resources/hbase when it is missing), the Hive
 * metastore is an embedded Derby database, and the table is created through the Hive driver,
 * as in beeline.
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 ./build/sbt -Pdocker-integration-tests \
 *     "docker-integration-tests/testOnly *HBaseStorageHandlerIntegrationSuite"
 * }}}
 */
@DockerTest
class HBaseStorageHandlerIntegrationSuite extends QueryTest with DockerIntegrationFunSuite {

  override protected val enableAutoThreadAudit = false

  private val imageName = sys.env.getOrElse("HBASE_DOCKER_IMAGE_NAME", "spark-test-hbase:2.6.3")
  private val hbaseTable = TableName.valueOf("status")
  private val family = Bytes.toBytes("d")
  private val qualifier = Bytes.toBytes("status")

  private var docker: DockerClient = _
  private var containerId: String = _
  private var hbase: Connection = _
  private var _spark: SparkSession = _

  override protected def spark: SparkSession = _spark

  override def beforeAll(): Unit = runIfTestsEnabled(s"Prepare for ${getClass.getName}") {
    super.beforeAll()
    try {
      _spark = createSparkSession(startHBase())
      Utils.tryWithResource(hbase.getAdmin)(_.createTable(TableDescriptorBuilder
        .newBuilder(hbaseTable).setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build()))
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
        .runSqlHive(
          """CREATE EXTERNAL TABLE status_hbase (key STRING, status STRING)
            |STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
            |WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,d:status')
            |TBLPROPERTIES ('hbase.table.name' = 'status')""".stripMargin)
    } catch {
      case NonFatal(e) =>
        try afterAll() finally throw e
    }
  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
      if (hbase != null) {
        hbase.close()
        hbase = null
      }
      if (containerId != null) {
        try {
          logContainerOutput()
        } finally {
          docker.removeContainerCmd(containerId).withForce(true).exec()
          containerId = null
        }
      }
    } finally {
      if (docker != null) {
        docker.close()
        docker = null
      }
      super.afterAll()
    }
  }

  test("SELECT from a Hive table stored in HBase") {
    Utils.tryWithResource(hbase.getTable(hbaseTable)) { table =>
      table.put(Seq("r1" -> "ready", "r2" -> "processing").map { case (key, status) =>
        new Put(Bytes.toBytes(key)).addColumn(family, qualifier, Bytes.toBytes(status))
      }.asJava)
    }
    checkAnswer(spark.sql("SELECT * FROM status_hbase WHERE key IN ('r1', 'r2')"),
      Seq(Row("r1", "ready"), Row("r2", "processing")))
  }

  test("INSERT from Spark into a Hive table stored in HBase") {
    spark.sql("INSERT INTO status_hbase VALUES ('w1', 'written')")
    val stored = Utils.tryWithResource(hbase.getTable(hbaseTable)) { table =>
      table.get(new Get(Bytes.toBytes("w1"))).getValue(family, qualifier)
    }
    assert(Bytes.toString(stored) === "written")
    checkAnswer(spark.sql("SELECT status FROM status_hbase WHERE key = 'w1'"), Row("written"))
  }

  /** Starts HBase in Docker, connects to it and returns the client settings. */
  private def startHBase(): Map[String, String] = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder.build
    docker = DockerClientImpl.getInstance(config, new ZerodepDockerHttpClient.Builder()
      .dockerHost(config.getDockerHost).sslConfig(config.getSSLConfig).build())
    try {
      docker.inspectImageCmd(imageName).exec()
    } catch {
      case _: NotFoundException =>
        val dir = new File(getClass.getResource("/hbase/Dockerfile").toURI).getParentFile
        logInfo(s"Building Docker image $imageName from $dir")
        docker.buildImageCmd(dir).withTags(Set(imageName).asJava)
          .exec(new BuildImageResultCallback).awaitImageId(30, TimeUnit.MINUTES)
    }

    // HBase registers its servers as localhost:port in ZooKeeper, so the ports (ZooKeeper,
    // master, region server) are published under the same numbers.
    val ports = freePorts(3)
    containerId = docker.createContainerCmd(imageName)
      .withHostConfig(HostConfig.newHostConfig()
        .withPortBindings(ports.map(p => PortBinding.parse(s"$p:$p")).asJava))
      .withExposedPorts(ports.map(p => ExposedPort.tcp(p)).asJava)
      .withEnv(s"HBASE_ZK_PORT=${ports(0)}", s"HBASE_MASTER_PORT=${ports(1)}",
        s"HBASE_REGIONSERVER_PORT=${ports(2)}")
      .exec()
      .getId
    docker.startContainerCmd(containerId).exec()

    val settings = Map(
      "hbase.zookeeper.quorum" -> "localhost",
      "hbase.zookeeper.property.clientPort" -> ports(0).toString,
      "hbase.client.retries.number" -> "10")
    val conf = HBaseConfiguration.create()
    settings.foreach { case (key, value) => conf.set(key, value) }
    hbase = ConnectionFactory.createConnection(conf)
    // The master answers once it is initialized, that is, once meta is online.
    eventually(timeout(5.minutes), interval(2.seconds)) {
      Utils.tryWithResource(hbase.getAdmin)(_.listNamespaceDescriptors())
    }
    settings
  }

  private def createSparkSession(hbaseSettings: Map[String, String]): SparkSession = {
    val builder = SparkSession.builder()
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
    // Hive wants a world-writable (733) scratch dir and creates it so only when it is missing.
    val scratchDir = Utils.createTempDir(namePrefix = "scratch")
    scratchDir.delete()
    val hiveAndHBaseConf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby = true) ++
      hbaseSettings + ("hive.exec.scratchdir" -> scratchDir.toURI.toString)
    hiveAndHBaseConf.foreach {
      case (key, value) if key == StaticSQLConf.WAREHOUSE_PATH.key => builder.config(key, value)
      case (key, value) => builder.config(s"spark.hadoop.$key", value)
    }
    builder.getOrCreate()
  }

  private def freePorts(n: Int): Seq[Int] = {
    val sockets = Seq.fill(n)(new ServerSocket(0))
    try sockets.map(_.getLocalPort) finally sockets.foreach(_.close())
  }

  private def logContainerOutput(): Unit = {
    docker.logContainerCmd(containerId).withStdOut(true).withStdErr(true)
      .exec(new ResultCallback.Adapter[Frame] {
        override def onNext(frame: Frame): Unit = logInfo(s"HBase container: $frame")
      })
      .awaitCompletion(1, TimeUnit.MINUTES)
  }
}

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

package org.apache.spark.sql.connect.service

import java.io.{File, FileInputStream}
import java.security.KeyStore
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}

import scala.jdk.CollectionConverters._

import io.grpc.{ManagedChannel, Status, StatusRuntimeException}
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.netty.buffer.ByteBufAllocator
import io.netty.util.ReferenceCountUtil

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkEnv, SparkException, SSLOptions}
import org.apache.spark.connect.proto
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Tests TLS of the Spark Connect GRPC service, configured in the `spark.ssl.connect` namespace or
 * given to `SparkConnectService.startWithTls`.
 */
class SparkConnectServiceTlsSuite extends SharedSparkSession {

  private val password = "password"

  // The test certificates are issued to CN=Unknown without subject alternative names, so the TLS
  // clients connect to localhost with this authority to pass the host name verification.
  private val certificateAuthority = "Unknown"

  // TLS 1.2 cipher suites that HTTP/2 allows with the RSA key of the test key store.
  private val cipherSuite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
  private val otherCipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"

  private lazy val keyStore = copyAndGetResourceFile("keystore", ".jks")
  private lazy val trustStore = copyAndGetResourceFile("truststore", ".jks")
  private lazy val untrustedKeyStore = copyAndGetResourceFile("untrusted-keystore", ".jks")

  // The global switch must not turn on TLS for Spark Connect. It would also turn on TLS for the
  // web UI, which has no key store here.
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.ssl.enabled", "true")
      .set(UI_ENABLED, false)

  override def afterEach(): Unit = {
    try {
      stopService()
    } finally {
      super.afterEach()
    }
  }

  private def tlsConfs: Seq[(String, String)] = Seq(
    "spark.ssl.connect.enabled" -> "true",
    "spark.ssl.connect.keyStore" -> keyStore.getAbsolutePath,
    "spark.ssl.connect.keyStorePassword" -> password)

  private def clientAuthConfs: Seq[(String, String)] = Seq(
    "spark.ssl.connect.needClientAuth" -> "true",
    "spark.ssl.connect.trustStore" -> trustStore.getAbsolutePath,
    "spark.ssl.connect.trustStorePassword" -> password)

  // Starts the service with `SparkConnectService.start`, which reads the `spark.ssl.connect`
  // options.
  private def startService(confs: (String, String)*): Int = {
    withSparkEnvConfs(confs :+ (Connect.CONNECT_GRPC_BINDING_PORT.key -> "0"): _*) {
      SparkConnectService.start(spark.sparkContext)
    }
    SparkConnectService.localPort
  }

  // Starts the service with `SparkConnectService.startWithTls` and the server certificate of
  // the key store.
  private def startServiceWithTls(
      protocols: Seq[String] = Nil,
      cipherSuites: Seq[String] = Nil,
      confs: Seq[(String, String)] = Nil): Int = {
    var port = -1
    withSparkEnvConfs(confs :+ (Connect.CONNECT_GRPC_BINDING_PORT.key -> "0"): _*) {
      port = SparkConnectService.startWithTls(
        spark.sparkContext,
        keyManagerFactory(keyStore),
        protocols,
        cipherSuites)
    }
    port
  }

  private def stopService(): Unit = {
    SparkConnectService.stop()
    Option(SparkConnectService.listener).foreach(spark.sparkContext.removeSparkListener)
  }

  private def loadKeyStore(file: File): KeyStore = {
    val store = KeyStore.getInstance("JKS")
    Utils.tryWithResource(new FileInputStream(file)) { in =>
      store.load(in, password.toCharArray)
    }
    store
  }

  private def keyManagerFactory(file: File): KeyManagerFactory = {
    val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    factory.init(loadKeyStore(file), password.toCharArray)
    factory
  }

  private def tlsChannel(
      port: Int,
      clientKeyStore: Option[File] = None,
      protocol: Option[String] = None,
      clientCipherSuite: Option[String] = None): ManagedChannel = {
    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(loadKeyStore(trustStore))
    val sslContextBuilder = GrpcSslContexts.forClient().trustManager(trustManagerFactory)
    clientKeyStore.foreach(file => sslContextBuilder.keyManager(keyManagerFactory(file)))
    protocol.foreach(p => sslContextBuilder.protocols(p))
    clientCipherSuite.foreach(c => sslContextBuilder.ciphers(Seq(c).asJava))
    NettyChannelBuilder
      .forAddress("localhost", port)
      .sslContext(sslContextBuilder.build())
      .overrideAuthority(certificateAuthority)
      .build()
  }

  private def plaintextChannel(port: Int): ManagedChannel = {
    NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build()
  }

  // Runs a simple RPC that returns the Spark version of the server, then closes the channel.
  private def sparkVersion(channel: ManagedChannel): String = {
    try {
      val request = proto.AnalyzePlanRequest
        .newBuilder()
        .setUserContext(proto.UserContext.newBuilder().setUserId("tls-user").build())
        .setSessionId(UUID.randomUUID().toString)
        .setSparkVersion(proto.AnalyzePlanRequest.SparkVersion.newBuilder().build())
        .build()
      proto.SparkConnectServiceGrpc
        .newBlockingStub(channel)
        .withDeadlineAfter(30, TimeUnit.SECONDS)
        .analyzePlan(request)
        .getSparkVersion
        .getVersion
    } finally {
      channel.shutdownNow()
    }
  }

  private def assertUnavailable(channel: ManagedChannel): Unit = {
    val e = intercept[StatusRuntimeException] {
      sparkVersion(channel)
    }
    assert(e.getStatus.getCode === Status.Code.UNAVAILABLE)
  }

  // Asserts that the service serves TLS only.
  private def assertServesTls(port: Int): Unit = {
    assert(sparkVersion(tlsChannel(port)) === SPARK_VERSION)
    assertUnavailable(plaintextChannel(port))
  }

  // Asserts that the service serves plaintext only.
  private def assertServesPlaintext(port: Int): Unit = {
    assert(sparkVersion(plaintextChannel(port)) === SPARK_VERSION)
    assertUnavailable(tlsChannel(port))
  }

  test("spark.ssl.connect.enabled serves TLS only") {
    assertServesTls(startService(tlsConfs: _*))
  }

  test("spark.ssl.connect.needClientAuth requires a trusted client certificate") {
    val port = startService(tlsConfs ++ clientAuthConfs: _*)
    assertUnavailable(tlsChannel(port))
    assertUnavailable(tlsChannel(port, clientKeyStore = Some(untrustedKeyStore)))
    assert(sparkVersion(tlsChannel(port, clientKeyStore = Some(keyStore))) === SPARK_VERSION)
  }

  test("spark.ssl.connect.protocol and enabledAlgorithms restrict the TLS handshake") {
    val port = startService(
      tlsConfs ++ Seq(
        "spark.ssl.connect.protocol" -> "TLSv1.2",
        "spark.ssl.connect.enabledAlgorithms" -> cipherSuite): _*)
    assert(sparkVersion(tlsChannel(port, protocol = Some("TLSv1.2"))) === SPARK_VERSION)
    assertUnavailable(tlsChannel(port, protocol = Some("TLSv1.3")))
    assertUnavailable(
      tlsChannel(port, protocol = Some("TLSv1.2"), clientCipherSuite = Some(otherCipherSuite)))
  }

  test("spark.ssl.connect.protocol that is not a TLS protocol version fails the start") {
    val e = intercept[IllegalArgumentException] {
      startService(tlsConfs :+ ("spark.ssl.connect.protocol" -> "TLS"): _*)
    }
    assert(e.getMessage.contains("spark.ssl.connect.protocol"))
    assert(!SparkConnectService.isRunning)
  }

  test("spark.ssl.connect.enabledAlgorithms without a supported cipher suite fails the start") {
    // SSLOptions keeps unsupported algorithms in tests (SPARK_TESTING), so the service checks them.
    val e = intercept[IllegalArgumentException] {
      startService(tlsConfs :+ ("spark.ssl.connect.enabledAlgorithms" -> "ABC, DEF"): _*)
    }
    assert(e.getMessage.contains("spark.ssl.connect.enabledAlgorithms"))
    assert(!SparkConnectService.isRunning)
  }

  test("missing spark.ssl.connect.keyStore fails the start without revealing the password") {
    val keyStorePassword = "key-store-password"
    val e = intercept[IllegalArgumentException] {
      startService(
        "spark.ssl.connect.enabled" -> "true",
        "spark.ssl.connect.keyStorePassword" -> keyStorePassword)
    }
    assert(e.getMessage.contains("spark.ssl.connect.keyStore"))
    assert(!e.getMessage.contains(keyStorePassword))
    assert(!SparkConnectService.isRunning)
  }

  test("restarted service serves TLS only while spark.ssl.connect.enabled is true") {
    assertServesTls(startService(tlsConfs: _*))
    stopService()
    assertServesPlaintext(startService())
    stopService()
    assertServesTls(startService(tlsConfs: _*))
  }

  test("spark.ssl.enabled alone keeps Spark Connect in plaintext") {
    assert(SparkEnv.get.conf.getBoolean("spark.ssl.enabled", defaultValue = false))
    assertServesPlaintext(startService())
  }

  test("createSslContext follows the protocol, cipher suite and client authentication options") {
    val sslContext = SparkConnectService.createSslContext(
      SSLOptions(
        enabled = true,
        keyStore = Some(keyStore),
        keyStorePassword = Some(password),
        needClientAuth = true,
        trustStore = Some(trustStore),
        trustStorePassword = Some(password),
        protocol = Some("TLSv1.2"),
        enabledAlgorithms = Set(cipherSuite)))
    assert(sslContext.isServer)
    val engine = sslContext.newEngine(ByteBufAllocator.DEFAULT)
    try {
      assert(engine.getEnabledProtocols.contains("TLSv1.2"))
      assert(!engine.getEnabledProtocols.contains("TLSv1.3"))
      assert(engine.getEnabledCipherSuites.toSeq === Seq(cipherSuite))
      assert(engine.getNeedClientAuth)
    } finally {
      ReferenceCountUtil.release(engine)
    }
  }

  test("startWithTls serves TLS with the KeyManagerFactory and returns the bound port") {
    val port = startServiceWithTls()
    assert(port > 0)
    assert(port === SparkConnectService.localPort)
    // Clients are not asked for certificates.
    assertServesTls(port)
  }

  test("startWithTls ignores the spark.ssl.connect options") {
    val bogusKeyStore = new File(keyStore.getParentFile, UUID.randomUUID().toString)
    val ignoredConfs = Seq(
      "spark.ssl.connect.keyStore" -> bogusKeyStore.getAbsolutePath,
      "spark.ssl.connect.keyStorePassword" -> "bogus-password",
      "spark.ssl.connect.needClientAuth" -> "true",
      "spark.ssl.connect.protocol" -> "TLS",
      "spark.ssl.connect.enabledAlgorithms" -> "ABC, DEF")
    Seq("false", "true").foreach { enabled =>
      val confs = ignoredConfs :+ ("spark.ssl.connect.enabled" -> enabled)
      assertServesTls(startServiceWithTls(confs = confs))
      stopService()
    }
  }

  test("startWithTls protocols and cipherSuites restrict the TLS handshake") {
    val port = startServiceWithTls(protocols = Seq("TLSv1.2"), cipherSuites = Seq(cipherSuite))
    assert(sparkVersion(tlsChannel(port, protocol = Some("TLSv1.2"))) === SPARK_VERSION)
    assertUnavailable(tlsChannel(port, protocol = Some("TLSv1.3")))
    assertUnavailable(
      tlsChannel(port, protocol = Some("TLSv1.2"), clientCipherSuite = Some(otherCipherSuite)))
  }

  test("startWithTls fails fast with a protocol that is not a TLS protocol version") {
    val e = intercept[IllegalArgumentException] {
      startServiceWithTls(protocols = Seq("TLSv1.3", "TLS"))
    }
    assert(e.getMessage.contains("protocols"))
    assert(!SparkConnectService.isRunning)
  }

  test("startWithTls fails fast without a supported cipher suite") {
    val e = intercept[IllegalArgumentException] {
      startServiceWithTls(cipherSuites = Seq("ABC", "DEF"))
    }
    assert(e.getMessage.contains("cipherSuites"))
    assert(!SparkConnectService.isRunning)
  }

  test("startWithTls fails while the service is running") {
    val plaintextPort = startService()
    val e = intercept[IllegalStateException] {
      startServiceWithTls()
    }
    assert(e.getMessage.contains("already started"))
    // The running service is kept as it is.
    assertServesPlaintext(plaintextPort)
    stopService()

    val tlsPort = startServiceWithTls()
    intercept[IllegalStateException] {
      startServiceWithTls()
    }
    assertServesTls(tlsPort)
  }

  test("startWithTls shuts the server down when the start fails after the bind") {
    // The shared session has no web UI, so the service cannot attach its tab to it.
    intercept[SparkException] {
      startServiceWithTls(confs = Seq(UI_ENABLED.key -> "true"))
    }
    assert(!SparkConnectService.isRunning)
    assert(SparkConnectService.server.awaitTermination(1, TimeUnit.MINUTES))
  }

  test("start keeps the running service that startWithTls started") {
    val port = startServiceWithTls()
    // spark.ssl.connect.enabled is not set, so a new server would serve plaintext.
    assert(startService() === port)
    assertServesTls(port)
  }

  test("startWithTls starts the service again after stop") {
    assertServesTls(startServiceWithTls())
    stopService()
    assertServesTls(startServiceWithTls())
  }
}

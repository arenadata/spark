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

package org.apache.spark.deploy.k8s.submit

import java.io.{File, StringWriter}
import java.nio.charset.MalformedInputException
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.{Codec, Source}

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, KeyToPath}
import org.apache.commons.codec.binary.Base64

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{Config, Constants, KubernetesUtils}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH, KUBERNETES_NAMESPACE}
import org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR
import org.apache.spark.internal.Logging

private[spark] object KubernetesClientUtils extends Logging {

  type ConfigMapItem = (String, Boolean)

  // Config map name can be KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH chars at max.
  def configMapName(prefix: String): String = {
    val suffix = "-conf-map"
    s"${prefix.take(KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH - suffix.length)}$suffix"
  }

  val configMapNameExecutor: String = configMapName(s"spark-exec-${KubernetesUtils.uniqueID()}")

  val configMapNameDriver: String = configMapName(s"spark-drv-${KubernetesUtils.uniqueID()}")

  private def buildStringFromPropertiesMap(configMapName: String,
      propertiesMap: Map[String, String]): String = {
    val properties = new Properties()
    propertiesMap.foreach { case (k, v) =>
      properties.setProperty(k, v)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName")
    propertiesWriter.toString
  }

  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR.
   */
  def buildSparkConfDirFilesMap(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: Map[String, String]): Map[String, ConfigMapItem] = synchronized {
    val loadedConfFilesMap = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    // Add resolved spark conf to the loaded configuration files map.
    if (resolvedPropertiesMap.nonEmpty) {
      val resolvedProperties: String = KubernetesClientUtils
        .buildStringFromPropertiesMap(configMapName, resolvedPropertiesMap)
      loadedConfFilesMap ++ Map(Constants.SPARK_CONF_FILE_NAME -> (resolvedProperties, true))
    } else {
      loadedConfFilesMap
    }
  }

  def buildKeyToPathObjects(
                             confFilesMap: Map[String, ConfigMapItem],
                             isPlainText: Boolean): Seq[KeyToPath] = {
    confFilesMap.filter(a => a._2._2 == isPlainText).map {
      case (fileName: String, (_, _)) =>
        val filePermissionMode = 420  // 420 is decimal for octal literal 0644.
        new KeyToPath(fileName, filePermissionMode, fileName)
    }.toList.sortBy(x => x.getKey) // List is sorted to make mocking based tests work
  }

  /**
   * Build a Config Map that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods.
   */
  def buildConfigMap(configMapName: String, confFileMap: Map[String, (String, Boolean)],
      withLabels: Map[String, String] = Map()): ConfigMap = {
    val configMapNameSpace =
      confFileMap.getOrElse(KUBERNETES_NAMESPACE.key,
        (KUBERNETES_NAMESPACE.defaultValueString, true))
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .withNamespace(configMapNameSpace._1)
        .withLabels(withLabels.asJava)
        .endMetadata()
      .withImmutable(true)
      .addToData(confFileMap.collect{case (key, (value, true)) => key -> value}.asJava)
      .addToBinaryData(confFileMap.collect{case (key, (value, false)) => key -> value}.asJava)
      .build()
  }

  private def orderFilesBySize(confFiles: Seq[File]): Seq[File] = {
    val fileToFileSizePairs = confFiles.map(f => (f, f.getName.length + f.length()))
    // sort first by name and then by length, so that during tests we have consistent results.
    fileToFileSizePairs.sortBy(f => f._1).sortBy(f => f._2).map(_._1)
  }

  // exposed for testing
  private[submit] def loadSparkConfDirFiles(conf: SparkConf): Map[String, ConfigMapItem] = {
    val confDir = Option(conf.getenv(ENV_SPARK_CONF_DIR)).orElse(
      conf.getOption("spark.home").map(dir => s"$dir/conf"))
    val maxSize = conf.get(Config.CONFIG_MAP_MAXSIZE)
    if (confDir.isDefined) {
      val confFiles: Seq[File] = listConfFiles(confDir.get, maxSize)
      val orderedConfFiles = orderFilesBySize(confFiles)
      var truncatedMapSize: Long = 0
      val truncatedMap = mutable.HashMap[String, ConfigMapItem]()
      val skippedFiles = mutable.HashSet[String]()
      var source: Source = Source.fromString("") // init with empty source.
      for (file <- orderedConfFiles) {
        try {
          source = Source.fromFile(file)(Codec.UTF8)
          val (fileName, fileContent) = file.getName -> source.mkString
          if ((truncatedMapSize + fileName.length + fileContent.length) < maxSize) {
            truncatedMap.put(fileName, (fileContent, true))
            truncatedMapSize = truncatedMapSize + (fileName.length + fileContent.length)
          } else {
            skippedFiles.add(fileName)
          }
        } catch {
          case e: MalformedInputException =>
            logWarning(
              s"Unable to read a non UTF-8 encoded file ${file.getAbsolutePath}. " +
                s"Adding as binary...", e)
            val (fileName, fileContent) = file.getName ->
              Base64.encodeBase64String(Files.toByteArray(file))
            if ((truncatedMapSize + fileName.length + fileContent.length) < maxSize) {
              truncatedMap.put(fileName, (fileContent, false))
              truncatedMapSize = truncatedMapSize + (fileName.length + fileContent.length)
            } else {
              skippedFiles.add(fileName)
            }
        } finally {
          source.close()
        }
      }
      if (truncatedMap.nonEmpty) {
        logInfo(s"Spark configuration files loaded from $confDir :" +
          s" ${truncatedMap.keys.mkString(",")}")
      }
      if (skippedFiles.nonEmpty) {
        logWarning(s"Skipped conf file(s) ${skippedFiles.mkString(",")}, due to size constraint." +
          s" Please see, config: `${Config.CONFIG_MAP_MAXSIZE.key}` for more details.")
      }
      truncatedMap.toMap
    } else {
      Map.empty[String, ConfigMapItem]
    }
  }

  private def listConfFiles(confDir: String, maxSize: Long): Seq[File] = {
    // At the moment configmaps do not support storing binary content (i.e. skip jar,tar,gzip,zip),
    // and configMaps do not allow for size greater than 1.5 MiB(configurable).
    // https://etcd.io/docs/v3.4.0/dev-guide/limit/
    def testIfTooLargeOrBinary(f: File): Boolean = (f.length() + f.getName.length > maxSize) ||
      f.getName.matches(".*\\.(gz|zip|jar|tar)")

    // We exclude all the template files and user provided spark conf or properties,
    // Spark properties are resolved in a different step.
    def testIfSparkConfOrTemplates(f: File) = f.getName.matches(".*\\.template") ||
      f.getName.matches("spark.*(conf|properties)")

    val fileFilter = (f: File) => {
      f.isFile && f.canRead && !testIfTooLargeOrBinary(f) && !testIfSparkConfOrTemplates(f)
    }
    val confFiles: Seq[File] = {
      val dir = new File(confDir)
      if (dir.isDirectory) {
        dir.listFiles.filter(x => fileFilter(x)).toSeq
      } else {
        Nil
      }
    }
    confFiles
  }
}

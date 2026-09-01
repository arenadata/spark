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
import java.nio.file.Files
import java.util.{List => JList, Map => JMap}
import java.util.{Base64, Properties}

import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, KeyToPath}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{DeveloperApi, Since, Stable}
import org.apache.spark.deploy.k8s.{Config, Constants, KubernetesUtils}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH, KUBERNETES_NAMESPACE}
import org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, PATH, PATHS}
import org.apache.spark.util.ArrayImplicits._

/**
 * An entry of a Spark conf directory ConfigMap.
 *
 * @param content file content, verbatim for plain text files and base64 encoded for binary ones
 * @param isPlainText whether the file is valid UTF-8 text, i.e. whether `content` belongs to the
 *                    ConfigMap `data` (`true`) or to its `binaryData` (`false`)
 */
private[spark] case class ConfigMapItem(content: String, isPlainText: Boolean)

/**
 * :: DeveloperApi ::
 *
 * A utility class used for K8s operations internally and Spark K8s operator.
 */
@Stable
@DeveloperApi
@Since("3.1.0")
object KubernetesClientUtils extends Logging {

  // Config map name can be KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH chars at max.
  @Since("3.3.0")
  def configMapName(prefix: String): String = {
    val suffix = "-conf-map"
    s"${prefix.take(KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH - suffix.length)}$suffix"
  }

  @Since("3.1.0")
  val configMapNameExecutor: String = configMapName(s"spark-exec-${KubernetesUtils.uniqueID()}")

  @Since("3.1.0")
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
   * (Java-friendly)
   */
  @Since("4.1.0")
  def buildSparkConfDirFilesMapJava(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: JMap[String, String]): JMap[String, String] = synchronized {
    buildSparkConfDirFilesMap(configMapName, sparkConf, resolvedPropertiesMap.asScala.toMap).asJava
  }

  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR.
   */
  @Since("3.1.1")
  def buildSparkConfDirFilesMap(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: Map[String, String]): Map[String, String] = synchronized {
    // Binary files cannot be represented in this map, they are dropped for backwards
    // compatibility. Use `buildSparkConfDirFilesMapWithBinary` to get them as well.
    buildSparkConfDirFilesMapWithBinary(configMapName, sparkConf, resolvedPropertiesMap)
      .collect { case (fileName, ConfigMapItem(content, true)) => fileName -> content }
  }

  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR, keeping
   * files that are not valid UTF-8 text as base64 encoded `binaryData` entries.
   */
  private[spark] def buildSparkConfDirFilesMapWithBinary(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: Map[String, String]): Map[String, ConfigMapItem] = synchronized {
    val loadedConfFilesMap = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    // Add resolved spark conf to the loaded configuration files map.
    if (resolvedPropertiesMap.nonEmpty) {
      val resolvedProperties: String = KubernetesClientUtils
        .buildStringFromPropertiesMap(configMapName, resolvedPropertiesMap)
      loadedConfFilesMap ++
        Map(Constants.SPARK_CONF_FILE_NAME -> ConfigMapItem(resolvedProperties, true))
    } else {
      loadedConfFilesMap
    }
  }

  @Since("4.1.0")
  def buildKeyToPathObjectsJava(confFilesMap: JMap[String, String]): JList[KeyToPath] = {
    buildKeyToPathObjects(confFilesMap.asScala.toMap).asJava
  }

  @Since("3.1.0")
  def buildKeyToPathObjects(confFilesMap: Map[String, String]): Seq[KeyToPath] = {
    buildKeyToPathObjectsFromNames(confFilesMap.keys)
  }

  /**
   * Same as `buildKeyToPathObjects`, for a map that also carries binary entries. Both plain text
   * and binary entries are mounted from the same ConfigMap, so both are included.
   */
  private[spark] def buildKeyToPathObjectsWithBinary(
      confFilesMap: Map[String, ConfigMapItem]): Seq[KeyToPath] = {
    buildKeyToPathObjectsFromNames(confFilesMap.keys)
  }

  private def buildKeyToPathObjectsFromNames(fileNames: Iterable[String]): Seq[KeyToPath] = {
    fileNames.map { fileName =>
      val filePermissionMode = 420  // 420 is decimal for octal literal 0644.
      new KeyToPath(fileName, filePermissionMode, fileName)
    }.toList.sortBy(x => x.getKey) // List is sorted to make mocking based tests work
  }

  /**
   * Build a ConfigMap that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods. (Java-friendly)
   */
  @Since("4.1.0")
  def buildConfigMapJava(configMapName: String, confFileMap: JMap[String, String],
      withLabels: JMap[String, String]): ConfigMap = {
    buildConfigMap(configMapName, confFileMap.asScala.toMap, withLabels.asScala.toMap)
  }

  /**
   * Build a Config Map that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods.
   */
  @Since("3.1.0")
  def buildConfigMap(configMapName: String, confFileMap: Map[String, String],
      withLabels: Map[String, String] = Map()): ConfigMap = {
    buildConfigMapWithBinary(configMapName,
      confFileMap.map { case (k, v) => k -> ConfigMapItem(v, true) }, withLabels)
  }

  /**
   * Same as `buildConfigMap`, for a map that also carries binary entries. Plain text entries go
   * into the ConfigMap `data`, base64 encoded binary ones into its `binaryData`.
   */
  private[spark] def buildConfigMapWithBinary(
      configMapName: String,
      confFileMap: Map[String, ConfigMapItem],
      withLabels: Map[String, String] = Map()): ConfigMap = {
    val configMapNameSpace = confFileMap.get(KUBERNETES_NAMESPACE.key)
      .map(_.content).getOrElse(KUBERNETES_NAMESPACE.defaultValueString)
    val binaryData = confFileMap.collect {
      case (key, ConfigMapItem(content, false)) => key -> content
    }
    val builder = new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .withNamespace(configMapNameSpace)
        .withLabels(withLabels.asJava)
        .endMetadata()
      .withImmutable(true)
      .addToData(confFileMap.collect {
        case (key, ConfigMapItem(content, true)) => key -> content
      }.asJava)
    // Left untouched when there is nothing binary to add, so that a ConfigMap built out of plain
    // text files only stays identical to what it was before binary files were supported.
    if (binaryData.nonEmpty) {
      builder.addToBinaryData(binaryData.asJava)
    }
    builder.build()
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
      def putIfFits(fileName: String, item: ConfigMapItem): Unit = {
        if ((truncatedMapSize + fileName.length + item.content.length) < maxSize) {
          truncatedMap.put(fileName, item)
          truncatedMapSize = truncatedMapSize + (fileName.length + item.content.length)
        } else {
          skippedFiles.add(fileName)
        }
      }
      for (file <- orderedConfFiles) {
        try {
          source = Source.fromFile(file)(Codec.UTF8)
          putIfFits(file.getName, ConfigMapItem(source.mkString, true))
        } catch {
          case e: MalformedInputException =>
            // Non UTF-8 files, keystores for example, would be corrupted if they went into the
            // ConfigMap `data`, so they are base64 encoded into its `binaryData` instead.
            logWarning(log"Unable to read a non UTF-8 encoded file " +
              log"${MDC(PATH, file.getAbsolutePath)}. Adding as binary...", e)
            putIfFits(file.getName,
              ConfigMapItem(Base64.getEncoder.encodeToString(Files.readAllBytes(file.toPath)),
                false))
        } finally {
          source.close()
        }
      }
      if (truncatedMap.nonEmpty) {
        logInfo(log"Spark configuration files loaded from ${MDC(PATH, confDir)} : " +
          log"${MDC(PATHS, truncatedMap.keys.mkString(","))}")
      }
      if (skippedFiles.nonEmpty) {
        logWarning(log"Skipped conf file(s) ${MDC(PATHS, skippedFiles.mkString(","))}, due to " +
          log"size constraint. Please see, config: " +
          log"`${MDC(CONFIG, Config.CONFIG_MAP_MAXSIZE.key)}` for more details.")
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
        dir.listFiles.filter(x => fileFilter(x)).toImmutableArraySeq
      } else {
        Nil
      }
    }
    confFiles
  }
}

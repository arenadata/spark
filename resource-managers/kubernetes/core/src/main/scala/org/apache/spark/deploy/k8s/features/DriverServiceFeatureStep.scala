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
package org.apache.spark.deploy.k8s.features

import scala.jdk.CollectionConverters._
import scala.util.Try

import io.fabric8.kubernetes.api.model.{HasMetadata, ServiceBuilder, ServicePort, ServicePortBuilder}

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH, KUBERNETES_DRIVER_SERVICE_IP_FAMILIES, KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY, KUBERNETES_DRIVER_SERVICE_PORT_PREFIX, KUBERNETES_DRIVER_SERVICE_PUBLISH_NOT_READY_ADDRESSES}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}

private[spark] class DriverServiceFeatureStep(
    kubernetesConf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep with Logging {
  import DriverServiceFeatureStep._

  require(kubernetesConf.getOption(DRIVER_BIND_ADDRESS_KEY).isEmpty,
    s"$DRIVER_BIND_ADDRESS_KEY is not supported in Kubernetes mode, as the driver's bind " +
      "address is managed and set to the driver pod's IP address.")
  require(kubernetesConf.getOption(DRIVER_HOST_KEY).isEmpty,
    s"$DRIVER_HOST_KEY is not supported in Kubernetes mode, as the driver's hostname will be " +
      "managed via a Kubernetes service.")

  private val resolvedServiceName = kubernetesConf.driverServiceName
  private val ipFamilyPolicy =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY)
  private val ipFamilies =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_IP_FAMILIES).split(",").toList.asJava
  private val publishNotReadyAddresses =
    kubernetesConf.sparkConf.get(KUBERNETES_DRIVER_SERVICE_PUBLISH_NOT_READY_ADDRESSES)

  private val driverPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_PORT.key, DEFAULT_DRIVER_PORT)
  private val driverBlockManagerPort = kubernetesConf.sparkConf.getInt(
    config.DRIVER_BLOCK_MANAGER_PORT.key, DEFAULT_BLOCKMANAGER_PORT)
  private val  driverUIPort = kubernetesConf.get(config.UI.UI_PORT)
  private val driverSparkConnectServerPort = kubernetesConf.sparkConf.getInt(
    CONNECT_GRPC_BINDING_PORT, DEFAULT_SPARK_CONNECT_SERVER_PORT)

  private val builtInPorts = Seq(
    DRIVER_PORT_NAME -> driverPort,
    BLOCK_MANAGER_PORT_NAME -> driverBlockManagerPort,
    UI_PORT_NAME -> driverUIPort,
    SPARK_CONNECT_SERVER_PORT_NAME -> driverSparkConnectServerPort)
  private val extraPorts = resolveExtraPorts(kubernetesConf.servicePorts, builtInPorts)

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val driverHostname = s"$resolvedServiceName.${kubernetesConf.namespace}.svc"
    Map(DRIVER_HOST_KEY -> driverHostname,
      config.DRIVER_PORT.key -> driverPort.toString,
      config.DRIVER_BLOCK_MANAGER_PORT.key -> driverBlockManagerPort.toString)
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val driverService = new ServiceBuilder()
      .withNewMetadata()
        .withName(resolvedServiceName)
        .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
        .addToLabels(SPARK_APP_ID_LABEL, kubernetesConf.appId)
        .addToLabels(kubernetesConf.serviceLabels.asJava)
        .endMetadata()
      .withNewSpec()
        .withClusterIP("None")
        .withPublishNotReadyAddresses(publishNotReadyAddresses)
        .withIpFamilyPolicy(ipFamilyPolicy)
        .withIpFamilies(ipFamilies)
        .withSelector(kubernetesConf.labels.asJava)
        .addNewPort()
          .withName(DRIVER_PORT_NAME)
          .withPort(driverPort)
          .withNewTargetPort(driverPort)
          .endPort()
        .addNewPort()
          .withName(BLOCK_MANAGER_PORT_NAME)
          .withPort(driverBlockManagerPort)
          .withNewTargetPort(driverBlockManagerPort)
          .endPort()
        .addNewPort()
          .withName(UI_PORT_NAME)
          .withPort(driverUIPort)
          .withNewTargetPort(driverUIPort)
          .endPort()
        .addNewPort()
          .withName(SPARK_CONNECT_SERVER_PORT_NAME)
          .withPort(driverSparkConnectServerPort)
          .withNewTargetPort(driverSparkConnectServerPort)
          .withAppProtocol("grpc")
          .endPort()
        .addAllToPorts(extraPorts.asJava)
        .endSpec()
      .build()
    Seq(driverService)
  }
}

private[spark] object DriverServiceFeatureStep {
  val DRIVER_BIND_ADDRESS_KEY = config.DRIVER_BIND_ADDRESS.key
  val DRIVER_HOST_KEY = config.DRIVER_HOST_ADDRESS.key
  val DRIVER_SVC_POSTFIX = "-driver-svc"
  val MAX_SERVICE_NAME_LENGTH = KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH
  // IANA_SVC_NAME, as Kubernetes requires for a port name: at most 15 lowercase alphanumerics
  // or '-', at least one letter, no leading, trailing or adjacent '-'.
  private val PORT_NAME_PATTERN = "^(?=.*[a-z])[a-z0-9]+(-[a-z0-9]+)*$".r
  private val MAX_PORT_NAME_LENGTH = 15

  /**
   * The ports from `spark.kubernetes.driver.service.port.<name>=<number>`, sorted by name, each
   * with the same target port. They are checked here, before the driver pod is created: a port
   * Kubernetes would refuse, or one clashing with a port Spark declares itself, fails submission.
   */
  private[features] def resolveExtraPorts(
      requested: Map[String, String],
      builtIn: Seq[(String, Int)]): Seq[ServicePort] = {
    val builtInNames = builtIn.map(_._1).toSet
    val builtInNumbers = builtIn.map(_._2).toSet
    val ports = requested.toSeq.sortBy(_._1).map { case (name, value) =>
      val key = s"$KUBERNETES_DRIVER_SERVICE_PORT_PREFIX$name"
      val validName =
        name.length <= MAX_PORT_NAME_LENGTH && PORT_NAME_PATTERN.findFirstIn(name).isDefined
      require(validName,
        s"$key: '$name' is not a valid port name; it must be at most $MAX_PORT_NAME_LENGTH " +
          "lowercase letters, digits or '-', contain a letter, and not start, end or repeat '-'.")
      require(!builtInNames.contains(name),
        s"$key: '$name' is a port the driver service already declares.")
      val number = Try(value.trim.toInt).toOption.filter(n => n >= 1 && n <= 65535)
      require(number.isDefined, s"$key: '$value' is not a port number between 1 and 65535.")
      require(!builtInNumbers.contains(number.get),
        s"$key: port ${number.get} is already declared by the driver service as " +
          s"${builtIn.find(_._2 == number.get).get._1}.")
      name -> number.get
    }
    ports.groupBy(_._2).foreach { case (number, sameNumber) =>
      require(sameNumber.size == 1,
        s"Driver service ports ${sameNumber.map(_._1).mkString(", ")} all declare port $number.")
    }
    ports.map { case (name, number) =>
      new ServicePortBuilder().withName(name).withPort(number).withNewTargetPort(number).build()
    }
  }
}

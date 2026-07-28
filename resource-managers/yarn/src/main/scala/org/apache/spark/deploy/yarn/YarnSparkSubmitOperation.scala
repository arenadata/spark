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
package org.apache.spark.deploy.yarn

import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{SparkHadoopUtil, SparkSubmitOperation}
import org.apache.spark.internal.config.{KEYTAB, PRINCIPAL}
import org.apache.spark.util.CommandLineLoggingUtils

/**
 * Implementation of [[SparkSubmitOperation]] for YARN, backing
 * `spark-submit --kill <appId>` and `spark-submit --status <appId>` with `--master yarn`.
 *
 * Unlike the submit path, spark-submit performs no Kerberos login before a kill or status
 * request, so on a secure cluster the request authenticates with the current ticket cache.
 * When spark.kerberos.principal and spark.kerberos.keytab are set, they are used to log in
 * instead.
 */
private[spark] class YarnSparkSubmitOperation extends SparkSubmitOperation
  with CommandLineLoggingUtils {

  private def withYarnClient(conf: SparkConf)(f: YarnClient => Unit): Unit = {
    val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(conf))
    (conf.get(PRINCIPAL), conf.get(KEYTAB)) match {
      case (Some(principal), Some(keytab)) =>
        SparkHadoopUtil.get.loginUserFromKeytab(principal, keytab)
      case (Some(_), None) =>
        printErrorAndExit("Keytab must be specified when principal is specified.")
      case (None, Some(_)) =>
        printErrorAndExit("Principal must be specified when keytab is specified.")
      case _ =>
    }
    val yarnClient = YarnClient.createYarnClient()
    try {
      yarnClient.init(hadoopConf)
      yarnClient.start()
      f(yarnClient)
    } finally {
      yarnClient.stop()
    }
  }

  override def kill(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"Submitting a request to kill submission $submissionId" +
      s" in ${conf.get("spark.master")}.")
    try {
      val appId = ApplicationId.fromString(submissionId)
      withYarnClient(conf) { yarnClient =>
        yarnClient.killApplication(appId)
        printMessage(s"Killed application $submissionId.")
      }
    } catch {
      case _: IllegalArgumentException =>
        printErrorAndExit(s"Submission ID: $submissionId is invalid.")
      case _: ApplicationNotFoundException =>
        printErrorAndExit(s"Application $submissionId not found.")
      case NonFatal(e) =>
        printErrorAndExit(s"Failed to kill application $submissionId: $e")
    }
  }

  override def printSubmissionStatus(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"Submitting a request for the status of submission $submissionId" +
      s" in ${conf.get("spark.master")}.")
    try {
      val appId = ApplicationId.fromString(submissionId)
      withYarnClient(conf) { yarnClient =>
        val report = yarnClient.getApplicationReport(appId)
        printMessage(s"Application status: ${formatReportDetails(report)}")
      }
    } catch {
      case _: IllegalArgumentException =>
        printErrorAndExit(s"Submission ID: $submissionId is invalid.")
      case _: ApplicationNotFoundException =>
        printErrorAndExit(s"Application $submissionId not found.")
      case NonFatal(e) =>
        printErrorAndExit(s"Failed to request status of application $submissionId: $e")
    }
  }

  private def formatReportDetails(report: ApplicationReport): String = {
    val details = Seq[(String, String)](
      ("state", report.getYarnApplicationState.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser),
      ("diagnostics", report.getDiagnostics)
    )

    // Use more loggable format if value is null or empty
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }

  override def supports(master: String): Boolean = {
    master.startsWith("yarn")
  }
}

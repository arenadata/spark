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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys.{HOST, PORT}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.internal.SQLConf

/**
 * The Spark Connect server
 */
object SparkConnectServer extends Logging {
  def main(args: Array[String]): Unit = {
    // Set the active Spark Session, and starts SparkEnv instance (via Spark Context)
    val conf = new SparkConf
    initSecurity(conf)
    logInfo("Starting Spark session.")
    val session = SparkSession
      .builder()
      .config(conf)
      .config(SQLConf.ARTIFACTS_SESSION_ISOLATION_ENABLED.key, true)
      .config(SQLConf.ARTIFACTS_SESSION_ISOLATION_ALWAYS_APPLY_CLASSLOADER.key, true)
      .getOrCreate()
    try {
      try {
        SparkConnectService.start(session.sparkContext)
        val isa = SparkConnectService.bindingAddress
        logInfo(
          log"Spark Connect server started at: " +
            log"${MDC(HOST, isa.getAddress.getHostAddress)}:${MDC(PORT, isa.getPort)}")
      } catch {
        case e: Exception =>
          logError("Error starting Spark Connect server", e)
          System.exit(-1)
      }
      SparkConnectService.server.awaitTermination()
    } finally {
      if (SparkConnectService.started) {
        SparkConnectService.stop()
      }
      session.stop()
    }
  }

  private def initSecurity(conf: SparkConf): Unit = {
    if (conf.contains(Connect.KERBEROS_KEYTAB)) {
      // if you have enabled kerberos the following 2 params must be set
      val keytabFilename = conf.get(Connect.KERBEROS_KEYTAB)
        .getOrElse(throw new NoSuchElementException(Connect.KERBEROS_KEYTAB.key))
      val principalName = conf.get(Connect.KERBEROS_PRINCIPAL)
        .getOrElse(throw new NoSuchElementException(Connect.KERBEROS_PRINCIPAL.key))

      conf.set(config.KEYTAB.key, keytabFilename)
      conf.set(config.PRINCIPAL.key, principalName)

      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }
}

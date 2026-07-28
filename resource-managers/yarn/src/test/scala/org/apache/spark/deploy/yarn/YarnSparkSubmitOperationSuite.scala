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

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.util.control.ControlThrowable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.{KEYTAB, PRINCIPAL}

class YarnSparkSubmitOperationSuite extends SparkFunSuite {

  // stops the operation at the first exitFn call without matching its NonFatal catches
  private class ExitCalled(val code: Int) extends ControlThrowable

  test("supports only yarn masters") {
    val op = new YarnSparkSubmitOperation
    assert(op.supports("yarn"))
    assert(!op.supports("k8s://host:443"))
    assert(!op.supports("spark://host:7077"))
    assert(!op.supports("local"))
  }

  test("kill rejects an invalid submission id") {
    val op = new YarnSparkSubmitOperation
    val out = new ByteArrayOutputStream()
    var exitCode = -1
    op.printStream = new PrintStream(out)
    op.exitFn = (code, _) => exitCode = code
    op.kill("not-an-app-id", new SparkConf(false).set("spark.master", "yarn"))
    assert(exitCode === 1)
    assert(new String(out.toByteArray).contains("is invalid"))
  }

  test("status rejects an invalid submission id") {
    val op = new YarnSparkSubmitOperation
    val out = new ByteArrayOutputStream()
    var exitCode = -1
    op.printStream = new PrintStream(out)
    op.exitFn = (code, _) => exitCode = code
    op.printSubmissionStatus("application_bad", new SparkConf(false).set("spark.master", "yarn"))
    assert(exitCode === 1)
    assert(new String(out.toByteArray).contains("is invalid"))
  }

  test("partial Kerberos configuration is rejected") {
    Seq(
      (Some("user@EXAMPLE.COM"), None, "Keytab must be specified"),
      (None, Some("/path/to/user.keytab"), "Principal must be specified")
    ).foreach { case (principal, keytab, expectedError) =>
      val op = new YarnSparkSubmitOperation
      val out = new ByteArrayOutputStream()
      op.printStream = new PrintStream(out)
      op.exitFn = (code, _) => throw new ExitCalled(code)
      val conf = new SparkConf(false).set("spark.master", "yarn")
      principal.foreach(conf.set(PRINCIPAL, _))
      keytab.foreach(conf.set(KEYTAB, _))
      val exit = intercept[ExitCalled] {
        op.kill("application_1753000000000_0001", conf)
      }
      assert(exit.code === 1)
      assert(new String(out.toByteArray).contains(expectedError))
    }
  }
}

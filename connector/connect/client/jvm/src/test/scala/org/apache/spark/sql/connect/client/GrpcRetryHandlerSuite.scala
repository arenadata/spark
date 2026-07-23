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
package org.apache.spark.sql.connect.client

import scala.concurrent.duration._

import io.grpc.{Status, StatusRuntimeException}

import org.apache.spark.sql.test.ConnectFunSuite

class GrpcRetryHandlerSuite extends ConnectFunSuite {

  // No-sleep policy so tests run instantly.
  private val policy = GrpcRetryHandler.RetryPolicy(
    maxRetries = 3,
    initialBackoff = 0.millis,
    maxBackoff = 0.millis,
    backoffMultiplier = 1.0,
    jitter = 0.millis,
    minJitterThreshold = 1.hour)

  private val noSleep: Long => Unit = _ => ()

  test("RetryException is retried and fn succeeds on second attempt") {
    // Reproduces the HA failover bug: Kyuubi crashes between queries.
    // ExecutePlanResponseReattachableIterator.callIter sets iter = Some(newExecutePlan)
    // then throws RetryException as a signal to re-enter the retry loop.
    // Without the fix, retry() had no case for RetryException so canRetry returned
    // false and the exception propagated to user code instead of retrying.
    var callCount = 0
    val result = GrpcRetryHandler.retry(policy, noSleep) {
      callCount += 1
      if (callCount == 1) throw new GrpcRetryHandler.RetryException
      "success"
    }
    assert(result == "success")
    assert(callCount == 2)
  }

  test("RetryException does not suppress a real error on the next attempt") {
    val realError = new StatusRuntimeException(Status.INTERNAL)
    var callCount = 0
    val ex = intercept[StatusRuntimeException] {
      GrpcRetryHandler.retry(policy, noSleep) {
        callCount += 1
        if (callCount == 1) throw new GrpcRetryHandler.RetryException
        throw realError
      }
    }
    assert(ex eq realError)
    assert(callCount == 2)
  }

  test("UNAVAILABLE is retried via canRetry") {
    val unavailable = new StatusRuntimeException(Status.UNAVAILABLE)
    var callCount = 0
    val result = GrpcRetryHandler.retry(policy, noSleep) {
      callCount += 1
      if (callCount <= 2) throw unavailable
      "ok"
    }
    assert(result == "ok")
    assert(callCount == 3)
  }

  test("non-retryable INTERNAL error propagates immediately") {
    val internalError = new StatusRuntimeException(Status.INTERNAL)
    var callCount = 0
    val ex = intercept[StatusRuntimeException] {
      GrpcRetryHandler.retry(policy, noSleep) {
        callCount += 1
        throw internalError
      }
    }
    assert(ex eq internalError)
    assert(callCount == 1)
  }
}

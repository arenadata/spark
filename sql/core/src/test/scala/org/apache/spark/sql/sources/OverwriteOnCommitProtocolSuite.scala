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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class OverwriteOnCommitProtocolSuite
  extends QueryTest
    with SharedSparkSession {

  import testImplicits._

  test("overwrite keeps old data until commit (successful job)") {
    withTempDir { dir =>
      val target = new File(dir, "tbl").getAbsolutePath

      Seq(1, 2, 3).toDF("id").write.mode("overwrite").parquet(target)
      // old data exists
      val before = spark.read.parquet(target).as[Int].collect().toSeq
      assert(before.sorted == Seq(1, 2, 3))

      // Overwrite with new data using deferred delete
      withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        "org.apache.spark.sql.execution.datasources.OverwriteOnCommitProtocol") {
        Seq(4, 5).toDF("id").write.mode("overwrite").parquet(target)
      }
      // After commit, new data visible, old data gone
      val after = spark.read.parquet(target).as[Int].collect().toSeq
      assert(after.sorted == Seq(4, 5))
    }
  }

  test("overwrite does not wipe target on job failure before commit") {
    withTempDir { dir =>
      val target = new File(dir, "tbl").getAbsolutePath
      Seq(1).toDF("id").write.mode("overwrite").parquet(target)

      val e = intercept[SparkException] {
        withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          "org.apache.spark.sql.execution.datasources.OverwriteOnCommitProtocol") {
          spark.range(0, 10).map { i =>
            if (i == 5) throw new RuntimeException("boom before commit") else i
          }.toDF("id").write.mode("overwrite").parquet(target)
        }
      }
      assert(
        Option(e.getMessage).exists(_.contains("boom before commit")) ||
          Option(e.getCause).
            exists(c => Option(c.getMessage).exists(_.contains("boom before commit")))
      )

      val afterFail = spark.read.parquet(target).as[Long].collect().toSeq
      assert(afterFail == Seq(1L))
    }
  }

  test("setupJob throws on FileOutputCommitter algorithm.version = 2") {
    withTempDir { dir =>
      val target = new java.io.File(dir, "tbl").getAbsolutePath
      Seq(1).toDF("id").write.mode("overwrite").parquet(target)

      val ex = intercept[UnsupportedOperationException] {
        withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          "org.apache.spark.sql.execution.datasources.OverwriteOnCommitProtocol") {
          Seq(2).toDF("id")
            .write
            .mode("overwrite")
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
            .parquet(target)
        }
      }

      assert(ex.getMessage.contains("requires FileOutputCommitter algorithm.version=1"))
    }
  }
}

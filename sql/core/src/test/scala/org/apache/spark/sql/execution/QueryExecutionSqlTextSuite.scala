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
package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Records the texts that check rules and optimizer rules see through [[QueryPlanningTracker.get]]:
 * the statement text and the text an audit event gets, the statement text or else the DataFrame
 * text.
 */
object SqlTextRecorder {
  private val seen = mutable.ArrayBuffer.empty[(String, Option[String], Option[String])]

  def record(phase: String): Unit = {
    val tracker = QueryPlanningTracker.get
    val sqlText = tracker.flatMap(_.sqlText)
    val text = sqlText.orElse(tracker.flatMap(_.dataFrameText))
    synchronized(seen += ((phase, sqlText, text)))
  }

  /** The statement texts seen in each phase while `f` runs. */
  def recorded[T](f: => T): Map[String, Set[Option[String]]] = {
    synchronized(seen.clear())
    f
    synchronized(seen.groupBy(_._1).map { case (phase, s) => phase -> s.map(_._2).toSet })
  }

  /** The texts of the audit events of the plans optimized while `f` runs. */
  def recordedTexts[T](f: => T): Set[Option[String]] = {
    synchronized(seen.clear())
    f
    synchronized(seen.filter(_._1 == "optimizer").map(_._3).toSet)
  }
}

class SqlTextRecorderExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectCheckRule(_ => _ => SqlTextRecorder.record("check"))
    extensions.injectOptimizerRule(_ => new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        SqlTextRecorder.record("optimizer")
        plan
      }
    })
  }
}

class QueryExecutionSqlTextSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SPARK_SESSION_EXTENSIONS.key, classOf[SqlTextRecorderExtension].getName)
    .set(SQLConf.SQL_STRING_REDACTION_PATTERN.key, "(?i)redact_[a-z0-9]+")

  private def optimizerSeen(f: => Unit): Set[Option[String]] =
    SqlTextRecorder.recorded(f).getOrElse("optimizer", Set.empty)

  private def withTestTable(f: => Unit): Unit = withTable("t") {
    sql("CREATE TABLE t (id INT, name STRING) USING parquet")
    sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    f
  }

  test("SQL statement text is visible to check and optimizer rules") {
    withTestTable {
      val q = "SELECT id FROM t WHERE id > 0"
      assert(sql(q).queryExecution.tracker.sqlText === Some(q))
      val seen = SqlTextRecorder.recorded(sql(q).collect())
      // Check rules also run for plans that are not queries, e.g. when an encoder is resolved.
      assert(seen("check").contains(Some(q)))
      assert(seen("check").flatten === Set(q))
      assert(seen("optimizer") === Set(Some(q)))
    }
  }

  test("statements whose optimized root is a relation keep their own text") {
    withTestTable {
      sql("SELECT id FROM t WHERE id = 777").collect()
      Seq("SELECT * FROM t", "TABLE t").foreach { q =>
        assert(optimizerSeen(sql(q).collect()) === Set(Some(q)))
      }
    }
  }

  test("Dataset actions on top of a SQL statement keep the statement text") {
    withTestTable {
      val q = "SELECT id, name FROM t WHERE id > 0"
      assert(optimizerSeen(sql(q).show()) === Set(Some(q)))
      assert(optimizerSeen(sql(q).count()) === Set(Some(q)))
      assert(optimizerSeen(sql(q).head()) === Set(Some(q)))
      assert(optimizerSeen(sql(q).filter("id > 1").collect()) === Set(Some(q)))
    }
  }

  test("lazily executed statements keep their own text") {
    withTestTable {
      val q1 = "SELECT name FROM t WHERE id = 1"
      val q2 = "SELECT name FROM t WHERE id = 2"
      val df1 = sql(q1)
      val df2 = sql(q2)
      assert(optimizerSeen(df1.collect()) === Set(Some(q1)))
      assert(optimizerSeen(df2.collect()) === Set(Some(q2)))
    }
  }

  test("queries built with the Dataset API have no statement text") {
    withTestTable {
      sql("SELECT id FROM t WHERE id = 777").collect()
      assert(spark.table("t").queryExecution.tracker.sqlText.isEmpty)
      val seen = SqlTextRecorder.recorded(spark.table("t").collect())
      assert(seen("check").flatten.isEmpty)
      assert(seen("optimizer") === Set(None))
      assert(optimizerSeen(spark.range(3).count()) === Set(None))
    }
  }

  test("commands and the queries they run see the statement text") {
    withTable("t", "t2") {
      Seq(
        "CREATE TABLE t (id INT, name STRING) USING parquet",
        "INSERT INTO t VALUES (1, 'a')",
        "CREATE TABLE t2 USING parquet AS SELECT id, name FROM t",
        "INSERT INTO t2 SELECT id, name FROM t WHERE id = 1",
        "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT id FROM t",
        "DROP VIEW v").foreach { q =>
        assert(optimizerSeen(sql(q)) === Set(Some(q)), q)
      }
    }
  }

  test("text is never taken from views or relations shared with other sessions") {
    withTestTable {
      withGlobalTempView("gv") {
        val other = spark.newSession()
        val q = "SELECT id, name FROM t WHERE name = 'a'"
        sql(q).createOrReplaceGlobalTempView("gv")
        sql("CREATE OR REPLACE GLOBAL TEMP VIEW gv2 AS SELECT id FROM t WHERE name = 'b'")
        try {
          assert(optimizerSeen(other.table("global_temp.gv").collect()) === Set(None))
          assert(optimizerSeen(other.table("global_temp.gv").count()) === Set(None))
          assert(optimizerSeen(other.table("global_temp.gv2").collect()) === Set(None))
          val q2 = "SELECT * FROM global_temp.gv"
          assert(optimizerSeen(other.sql(q2).collect()) === Set(Some(q2)))
          val q3 = "SELECT * FROM t"
          assert(optimizerSeen(other.sql(q3).collect()) === Set(Some(q3)))
        } finally {
          sql("DROP VIEW IF EXISTS global_temp.gv2")
        }
      }
    }
  }

  test("statement text is redacted with the pattern of the application") {
    withTestTable {
      val q = "SELECT id FROM t WHERE name = 'redact_abc1'"
      val redacted = Set(Some("SELECT id FROM t WHERE name = '*********(redacted)'"))
      assert(optimizerSeen(sql(q).collect()) === redacted)
      // Sessions cannot turn the redaction off or redact the whole text.
      Seq("(?!)", "(?s).+").foreach { pattern =>
        withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> pattern) {
          assert(optimizerSeen(sql(q).collect()) === redacted, pattern)
        }
      }
    }
  }

  test("caches rebuilt by other sessions do not take the texts of the queries that cached them") {
    withTestTable {
      withTempView("c") {
        val other = spark.newSession()
        def checkNoSecret(texts: Set[Option[String]]): Unit = {
          assert(texts.nonEmpty, "no plan was optimized")
          texts.flatten.foreach(text => assert(!text.contains("secret_"), text))
        }
        try {
          // Never materialized, so that every change of `t` rebuilds them.
          sql("SELECT id FROM t WHERE name <> 'secret_sql'").cache()
          spark.table("t").filter(col("name") =!= "secret_df").cache()
          sql("CACHE LAZY TABLE c AS SELECT id FROM t WHERE name <> 'secret_cache_table'")
          // Without a command, the rebuilt caches have no text.
          val refreshed = SqlTextRecorder.recordedTexts(other.catalog.refreshTable("t"))
          checkNoSecret(refreshed)
          assert(refreshed.contains(None))
          val unpersisted = SqlTextRecorder.recordedTexts(other.table("t").unpersist())
          checkNoSecret(unpersisted)
          assert(unpersisted === Set(None))
          // Under a command, they have the text of the command.
          val inserted = SqlTextRecorder.recordedTexts {
            other.range(1).selectExpr("CAST(id AS INT) AS id", "'x' AS name")
              .write.mode("append").insertInto("t")
          }
          checkNoSecret(inserted)
          assert(inserted.forall(_.exists(_.endsWith(""".write.insertInto("t")"""))), inserted)
          val refresh = "REFRESH TABLE t"
          assert(SqlTextRecorder.recordedTexts(other.sql(refresh)) === Set(Some(refresh)))
        } finally {
          spark.catalog.clearCache()
        }
      }
    }
  }

  test("queries run by a command have the texts of the command only") {
    withTestTable {
      try {
        sql("SELECT id FROM t WHERE name = 'a'").cache().count()
        // The write rebuilds the cache.
        val texts = SqlTextRecorder.recordedTexts {
          spark.range(1).selectExpr("CAST(id AS INT) AS id", "'x' AS name")
            .write.mode("append").insertInto("t")
        }
        assert(texts.nonEmpty && texts.forall(_.exists(_.endsWith(""".write.insertInto("t")"""))),
          texts)
        val insert = "INSERT INTO t VALUES (3, 'c')"
        assert(SqlTextRecorder.recordedTexts(sql(insert)) === Set(Some(insert)))
      } finally {
        spark.catalog.clearCache()
      }
    }
  }
}

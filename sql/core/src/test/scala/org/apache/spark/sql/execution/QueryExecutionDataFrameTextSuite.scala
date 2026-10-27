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

import java.util.Properties

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Dataset, QueryTest, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendData, Command, Filter, Join, JoinHint, LeafNode, LogicalPlan, Project, Range, SubqueryAlias, UnaryNode, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Metadata, StructType}

case class DataFrameTextOrder(order_id: Int, customer_id: Int, amount: Int, status: String)

/** Records the texts that optimizer rules see through [[QueryPlanningTracker.get]]. */
object DataFrameTextRecorder extends Rule[LogicalPlan] {
  private val seen = mutable.LinkedHashSet.empty[(String, Option[String], Option[String])]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    QueryPlanningTracker.get.foreach { tracker =>
      val texts = (Thread.currentThread().getName, tracker.sqlText, tracker.dataFrameText)
      synchronized(seen += texts)
    }
    plan
  }

  /** Returns the distinct (sqlText, dataFrameText) pairs of the plans optimized by `f`. */
  def recorded(f: => Unit): Seq[(Option[String], Option[String])] = {
    recordedByThread(f).map { case (_, sqlText, dataFrameText) => sqlText -> dataFrameText }
      .distinct
  }

  /** Returns the distinct (thread, sqlText, dataFrameText) of the plans optimized by `f`. */
  def recordedByThread(f: => Unit): Seq[(String, Option[String], Option[String])] = {
    synchronized(seen.clear())
    f
    synchronized(seen.toSeq)
  }
}

/** Nodes the renderer does not know, with subqueries. */
object UnknownNodes {
  case class Checked(condition: Expression, child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  case class Generated(condition: Expression) extends LeafNode {
    override def output: Seq[Attribute] = Nil
  }
}

/**
 * Analyzer rules like the ones of the Apache Kyuubi authorization plugin, for table `secured`:
 * a row filter and a data mask in markers of the same names, removed by an optimizer rule.
 */
object AuthzLikeRules {
  val ROW_FILTER = "status <> 'secret_policy'"
  val MASK = "md5(note)"

  case class RowFilterMarker(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  case class DataMaskingStage0Marker(child: LogicalPlan, scan: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  case class DataMaskingStage1Marker(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
      copy(child = newChild)
  }

  case class PermanentViewMarker(child: LogicalPlan, catalogTable: CatalogTable)
    extends LeafNode with MultiInstanceRelation {
    override def output: Seq[Attribute] = child.output
    override def newInstance(): LogicalPlan = copy()
  }

  object ApplyRowFilterAndMask extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      case _: RowFilterMarker | _: DataMaskingStage0Marker | _: DataMaskingStage1Marker => plan
      case r: LogicalRelation if r.catalogTable.exists(_.identifier.table == "secured") =>
        val masked = r.output.map {
          case a if a.name == "note" => Alias(CatalystSqlParser.parseExpression(MASK), a.name)()
          case a => a: NamedExpression
        }
        Filter(CatalystSqlParser.parseExpression(ROW_FILTER),
          RowFilterMarker(DataMaskingStage0Marker(Project(masked, r), r)))
      case _ => plan.withNewChildren(plan.children.map(apply))
    }
  }

  object ApplyMaskStage1 extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan match {
      // Dataset.join expects the analyzed plan of a join to be a Join.
      case _: DataMaskingStage1Marker | _: Command | _: Join => plan
      case _ if plan.resolved && plan.exists(_.isInstanceOf[DataMaskingStage0Marker]) =>
        DataMaskingStage1Marker(plan)
      case _ => plan
    }
  }

  object EliminateMarkers extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
      case m: RowFilterMarker => m.child
      case m: DataMaskingStage0Marker => m.child
      case m: DataMaskingStage1Marker => m.child
    }
  }
}

class AuthzLikeExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(_ => AuthzLikeRules.ApplyRowFilterAndMask)
    extensions.injectResolutionRule(_ => AuthzLikeRules.ApplyMaskStage1)
    extensions.injectOptimizerRule(_ => AuthzLikeRules.EliminateMarkers)
  }
}

class QueryExecutionDataFrameTextSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val derbyUrl = "jdbc:derby:memory:dataframetext;create=true"

  // Values that the texts must never have: data source options, row filters and masks, local
  // data and the queries of other sessions and of views.
  private val secrets = Seq("s3cr3t", "secret_policy", "md5", "secret_value", "secret_gv",
    "secret_view", "jdbc:derby")

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SPARK_SESSION_EXTENSIONS.key, classOf[AuthzLikeExtension].getName)
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set(SQLConf.SQL_STRING_REDACTION_PATTERN.key, "(?i)redact_[a-z0-9]+")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraOptimizations = Seq(DataFrameTextRecorder)
    sql("CREATE TABLE orders (order_id INT, customer_id INT, amount INT, status STRING) " +
      "USING parquet")
    sql("INSERT INTO orders VALUES (1, 10, 150, 'paid'), (2, 10, 50, 'new'), (3, 20, 500, 'paid')")
    sql("CREATE TABLE customers (customer_id INT, name STRING, city STRING) USING parquet")
    sql("INSERT INTO customers VALUES (10, 'Ann', 'Moscow'), (20, 'Bob', 'Kazan')")
    sql("CREATE TABLE secured (id INT, status STRING, note STRING) USING parquet")
    sql("INSERT INTO secured VALUES (1, 'open', 'a'), (2, 'secret_policy', 'b')")
  }

  override def afterAll(): Unit = {
    try {
      spark.experimental.extraOptimizations = Nil
      Seq("orders", "customers", "secured").foreach(t => sql(s"DROP TABLE IF EXISTS $t"))
    } finally {
      super.afterAll()
    }
  }

  private def orders = spark.table("orders")
  private def customers = spark.table("customers")

  /** The distinct (sqlText, dataFrameText) pairs of the plans optimized while `f` runs. */
  private def seen(f: => Unit): Seq[(Option[String], Option[String])] = {
    val texts = DataFrameTextRecorder.recorded(f)
    for ((_, Some(text)) <- texts; secret <- secrets) {
      assert(!text.contains(secret), s"'$secret' in $text")
    }
    texts
  }

  /** The DataFrame text of all plans optimized while `f` runs, which must have the same one. */
  private def dataFrameText(f: => Unit): String = {
    val texts = seen(f)
    assert(texts.nonEmpty, "no plan was optimized")
    assert(texts.forall(_._1.isEmpty), s"plans with SQL text: $texts")
    assert(texts.map(_._2).distinct.size === 1, s"plans with different texts: $texts")
    texts.head._2.getOrElse(fail("no DataFrame text"))
  }

  private def checkText(expected: String)(f: => Unit): Unit = {
    assert(dataFrameText(f) === expected)
  }

  private def render(plan: LogicalPlan): Option[String] = DataFrameText.render(plan)

  // Plans as the Spark Connect planner builds them from the plans its clients send: the nodes of
  // the plan of a DataFrame and the columns that refer to the DataFrame, e.g. df("id") or df.id
  // in Python, have the plan id of the DataFrame.
  private def withPlanId[T <: TreeNode[_]](node: T, id: Long): T = {
    node.setTagValue(LogicalPlan.PLAN_ID_TAG, id)
    node
  }

  private def connectTable(name: String, id: Long): LogicalPlan =
    withPlanId(UnresolvedRelation(Seq(name)), id)

  private def connectColumn(name: String, id: Long): UnresolvedAttribute =
    withPlanId(UnresolvedAttribute.quotedString(name), id)

  private def connectFunction(name: String, arguments: Expression*): Expression =
    UnresolvedFunction(name, arguments, isDistinct = false)

  test("table, filter and select") {
    checkText("""spark.table("orders").filter("amount > 100").select("customer_id", "amount")""") {
      orders.filter("amount > 100").select("customer_id", "amount").collect()
    }
    checkText("""spark.table("orders")""")(orders.collect())
  }

  test("filter with columns") {
    checkText(
      """spark.table("orders").filter("(amount > 100) AND (status = 'paid')")""") {
      orders.filter(col("amount") > 100 && $"status" === "paid").collect()
    }
    checkText(
      """spark.table("orders").filter("(status IN ('paid', 'new')) OR (status IS NULL)")""") {
      orders.where(col("status").isin("paid", "new") || col("status").isNull).collect()
    }
    checkText("""spark.table("orders").filter("NOT (status LIKE 'p%')")""") {
      orders.filter(!col("status").like("p%")).collect()
    }
  }

  test("filter with a SQL expression is shown like the same filter with columns") {
    checkText("""spark.table("orders").filter("(amount > 100) AND (status = 'paid')")""") {
      orders.filter("amount > 100 and status = 'paid'").collect()
    }
    checkText("""spark.table("orders").filter("NOT (status LIKE 'p%')")""") {
      orders.filter("status NOT LIKE 'p%'").collect()
    }
  }

  test("select columns and expressions") {
    checkText("""spark.table("orders").select("customer_id", "amount")""") {
      orders.select(col("customer_id"), $"amount").collect()
    }
    checkText("""spark.table("orders").selectExpr("customer_id", "(amount * 2) AS doubled", """ +
        """"upper(status)")""") {
      orders.select(col("customer_id"), (col("amount") * 2).as("doubled"), upper(col("status")))
        .collect()
    }
    checkText("""spark.table("orders").select("*")""")(orders.select("*").collect())
  }

  test("selectExpr") {
    checkText(
      """spark.table("orders").selectExpr("customer_id", "(amount * 2) AS doubled")""") {
      orders.selectExpr("customer_id", "amount * 2 AS doubled").collect()
    }
  }

  test("withColumn, withColumns, withColumnRenamed") {
    checkText("""spark.table("orders").withColumn("doubled", expr("amount * 2"))""") {
      orders.withColumn("doubled", col("amount") * 2).collect()
    }
    checkText("""spark.table("orders").withColumns(Map("one" -> expr("1"), """ +
        """"next" -> expr("amount + 1")))""") {
      orders.withColumns(Map("one" -> lit(1), "next" -> (col("amount") + 1))).collect()
    }
    checkText("""spark.table("orders").withColumnRenamed("amount", "total")""") {
      orders.withColumnRenamed("amount", "total").collect()
    }
  }

  test("drop") {
    // Dataset.drop(String) selects the remaining columns.
    checkText("""spark.table("orders").select("order_id", "customer_id", "amount")""") {
      orders.drop("status").collect()
    }
    checkText("""spark.table("orders").drop("status")""")(orders.drop(col("status")).collect())
  }

  test("toDF, as and typed views of the same query") {
    checkText("""spark.table("orders").toDF("a", "b", "c", "d")""") {
      orders.toDF("a", "b", "c", "d").collect()
    }
    checkText("""spark.table("orders")""")(orders.as[DataFrameTextOrder].toDF().collect())
  }

  test("groupBy and agg") {
    checkText(
      """spark.table("orders").groupBy("customer_id").agg(expr("sum(amount) AS total"))""") {
      orders.groupBy("customer_id").agg(sum("amount").as("total")).collect()
    }
    checkText("""spark.table("orders").groupBy("customer_id").count()""") {
      orders.groupBy("customer_id").count().collect()
    }
    checkText(
      """spark.table("orders").groupBy("customer_id", "status").agg(expr("sum(amount)"), """ +
        """expr("avg(amount) AS average"), expr("count(DISTINCT order_id)"))""") {
      orders.groupBy(col("customer_id"), col("status"))
        .agg(sum("amount"), avg("amount").as("average"), countDistinct("order_id")).collect()
    }
    checkText("""spark.table("orders").groupBy(expr("amount % 10")).count()""") {
      orders.groupBy(col("amount") % 10).count().collect()
    }
  }

  test("agg without groupBy and count") {
    checkText("""spark.table("orders").agg(expr("max(amount)"), expr("min(amount)"))""") {
      orders.agg(max("amount"), min("amount")).collect()
    }
    checkText("""spark.table("orders").filter("amount > 100").count()""") {
      orders.filter("amount > 100").count()
    }
  }

  test("rollup, cube and pivot") {
    checkText(
      """spark.table("orders").rollup("customer_id", "status").agg(expr("sum(amount)"))""") {
      orders.rollup("customer_id", "status").agg(sum("amount")).collect()
    }
    checkText("""spark.table("orders").cube("customer_id").count()""") {
      orders.cube("customer_id").count().collect()
    }
    checkText(
      """spark.table("orders").groupBy("customer_id").pivot("status", Seq('paid', 'new'))""" +
        """.agg(expr("sum(amount)"))""") {
      orders.groupBy("customer_id").pivot("status", Seq("paid", "new")).agg(sum("amount"))
        .collect()
    }
  }

  test("join with a condition") {
    checkText("""spark.table("orders").join(spark.table("customers"), """ +
        """expr("orders.customer_id = customers.customer_id"), "inner")""") {
      orders.join(customers, orders("customer_id") === customers("customer_id")).collect()
    }
    checkText("""spark.table("orders").join(spark.table("customers"), """ +
        """expr("orders.customer_id = customers.customer_id"), "left")""") {
      orders.join(customers, orders("customer_id") === customers("customer_id"), "left_outer")
        .collect()
    }
  }

  test("join on columns") {
    checkText("""spark.table("orders").join(spark.table("customers"), Seq("customer_id"), """ +
        """"inner")""") {
      orders.join(customers, Seq("customer_id")).collect()
    }
    checkText("""spark.table("orders").join(spark.table("customers"), Seq("customer_id"), """ +
        """"full")""") {
      orders.join(customers, Seq("customer_id"), "full").collect()
    }
  }

  test("semi, anti and cross joins") {
    checkText("""spark.table("orders").join(spark.table("customers"), """ +
        """expr("orders.customer_id = customers.customer_id"), "left_semi")""") {
      orders.join(customers, orders("customer_id") === customers("customer_id"), "semi")
        .collect()
    }
    checkText("""spark.table("orders").join(spark.table("customers"), """ +
        """expr("orders.customer_id = customers.customer_id"), "left_anti")""") {
      orders.join(customers, orders("customer_id") === customers("customer_id"), "anti")
        .collect()
    }
    checkText("""spark.table("orders").crossJoin(spark.table("customers"))""") {
      orders.crossJoin(customers).collect()
    }
  }

  test("self join with aliases") {
    checkText("""spark.table("orders").as("a").join(spark.table("orders").as("b"), """ +
        """expr("a.customer_id = b.customer_id"), "inner")""") {
      orders.as("a").join(orders.as("b"), col("a.customer_id") === col("b.customer_id"))
        .collect()
    }
    val o = orders
    checkText("""spark.table("orders").join(spark.table("orders"), Seq("order_id"), "inner")""") {
      o.join(o, Seq("order_id")).collect()
    }
  }

  test("union, unionByName, intersect and except") {
    checkText("""spark.table("orders").filter("amount > 100").union(spark.table("orders")""" +
        """.filter("amount < 10"))""") {
      orders.filter("amount > 100").union(orders.filter("amount < 10")).collect()
    }
    checkText(
      """spark.table("orders").select("customer_id").unionByName(spark.table("customers")""" +
        """.select("customer_id", "city"), allowMissingColumns = true)""") {
      orders.select("customer_id")
        .unionByName(customers.select("customer_id", "city"), allowMissingColumns = true)
        .collect()
    }
    checkText("""spark.table("orders").select("customer_id").intersect(spark.table("customers")""" +
        """.select("customer_id"))""") {
      orders.select("customer_id").intersect(customers.select("customer_id")).collect()
    }
    checkText("""spark.table("orders").select("customer_id").exceptAll(spark.table("customers")""" +
        """.select("customer_id"))""") {
      orders.select("customer_id").exceptAll(customers.select("customer_id")).collect()
    }
  }

  test("distinct and dropDuplicates") {
    checkText("""spark.table("orders").select("customer_id").distinct()""") {
      orders.select("customer_id").distinct().collect()
    }
    checkText("""spark.table("orders").dropDuplicates("customer_id", "status")""") {
      orders.dropDuplicates("status", "customer_id").collect()
    }
  }

  test("orderBy, sort, limit and offset") {
    checkText("""spark.table("orders").orderBy("amount DESC", "order_id").limit(2).offset(1)""") {
      orders.orderBy(col("amount").desc, col("order_id")).limit(2).offset(1).collect()
    }
    checkText("""spark.table("orders").orderBy("status NULLS LAST")""") {
      orders.sort(col("status").asc_nulls_last).collect()
    }
    checkText("""spark.table("orders").sortWithinPartitions("amount")""") {
      orders.sortWithinPartitions("amount").collect()
    }
  }

  test("window functions") {
    checkText("""spark.table("orders").withColumn("rn", expr("row_number() OVER """ +
        """(PARTITION BY customer_id ORDER BY amount DESC)"))""") {
      orders.withColumn("rn",
        row_number().over(Window.partitionBy("customer_id").orderBy(col("amount").desc)))
        .collect()
    }
    checkText("""spark.table("orders").withColumn("running", expr("sum(amount) OVER """ +
        """(PARTITION BY customer_id ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING """ +
        """AND CURRENT ROW)"))""") {
      orders.withColumn("running", sum("amount").over(Window.partitionBy("customer_id")
        .orderBy("order_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .collect()
    }
  }

  test("explode and higher-order functions") {
    checkText("""spark.range(3).selectExpr("id", "explode(array(id, 1)) AS v")""") {
      spark.range(3).select(col("id"), explode(array(col("id"), lit(1))).as("v")).collect()
    }
    val text = dataFrameText {
      spark.range(3).select(transform(array(col("id")), x => x + 1).as("t")).collect()
    }
    assert(text.matches(
      """spark\.range\(3\)\.selectExpr\("transform\(array\(id\), (\w+) -> \1 \+ 1\) AS t"\)"""),
      text)
  }

  test("Scala UDFs") {
    val anonymous = udf((s: String) => s.toUpperCase)
    checkText("""spark.table("orders").selectExpr("UDF(status)")""") {
      orders.select(anonymous(col("status"))).collect()
    }
    val named = udf((s: String) => s.toUpperCase).withName("to_upper")
    checkText("""spark.table("orders").selectExpr("to_upper(status) AS s")""") {
      orders.select(named(col("status")).as("s")).collect()
    }
    spark.udf.register("registered_upper", (s: String) => s.toUpperCase)
    checkText("""spark.table("orders").filter("registered_upper(status) = 'PAID'")""") {
      orders.filter(call_udf("registered_upper", col("status")) === "PAID").collect()
    }
  }

  test("typed Dataset operations") {
    val typed = orders.as[DataFrameTextOrder]
    checkText("""spark.table("orders").filter(<function>).map(<function>)""") {
      typed.filter(_.amount > 100).map(_.customer_id).collect()
    }
    checkText("""spark.table("orders").mapPartitions(<function>)""") {
      typed.mapPartitions(_.map(_.amount)).collect()
    }
    checkText("""spark.table("orders").groupByKey(<function>).mapGroups(<function>)""") {
      typed.groupByKey(_.customer_id).mapGroups((k, rows) => (k, rows.size)).collect()
    }
  }

  test("range") {
    checkText("""spark.range(10).filter("id > 5")""")(spark.range(10).filter("id > 5").collect())
    checkText("""spark.range(1, 10, 2)""")(spark.range(1, 10, 2).collect())
  }

  test("local data is not shown") {
    checkText("""spark.createDataFrame(<local data>).toDF("id", "name").filter("id > 0")""") {
      Seq((1, "secret_value")).toDF("id", "name").filter("id > 0").collect()
    }
    checkText("""spark.createDataFrame(<rdd>).filter("_1 > 0")""") {
      spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "secret_value"))))
        .filter("_1 > 0").collect()
    }
  }

  test("files are shown by path, without options") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(path)
      // Spark 3.5 DataFrameReader.load resolves file sources at once: their relations keep only
      // the qualified root paths, not the paths the user gave.
      checkText(s"""spark.read.format("parquet").load("file:$path").filter("id > 1")""") {
        spark.read.option("password", "s3cr3t").parquet(path).filter("id > 1").collect()
      }
      checkText(s"""spark.read.format("parquet").load("file:$path")""") {
        spark.read.format("parquet").option("path", path).load().collect()
      }
    }
  }

  test("JDBC reads and writes are shown without options") {
    val props = new Properties()
    props.put("user", "admin")
    props.put("password", "s3cr3t")
    checkText("""spark.range(3).write.format("jdbc").mode("overwrite").save()""") {
      spark.range(3).write.format("jdbc").option("url", derbyUrl).option("dbtable", "T1")
        .option("user", "admin").option("password", "s3cr3t").mode("overwrite").save()
    }
    checkText("""spark.range(2).write.format("jdbc").mode("append").save()""") {
      spark.range(2).write.mode("append").jdbc(derbyUrl, "T1", props)
    }
    checkText("""spark.read.format("jdbc").load().filter("id > 0")""") {
      spark.read.format("jdbc").option("url", derbyUrl).option("dbtable", "T1")
        .option("user", "admin").option("password", "s3cr3t").load().filter("id > 0").collect()
    }
    checkText("""spark.read.format("jdbc").load().groupBy("id").count()""") {
      spark.read.jdbc(derbyUrl, "T1", props).groupBy("id").count().collect()
    }
  }

  test("data source options and credentials in paths are never shown") {
    // Spark 3.5 has no UnresolvedDataSource: DataFrameReader.load resolves the relation at once,
    // so the relations it creates are rendered instead. JDBCOptions needs a registered driver.
    val options = CaseInsensitiveMap(Map("url" -> "jdbc:postgresql://db:5432/sales",
      "user" -> "admin", "password" -> "s3cr3t", "dbtable" -> "orders",
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver"))
    val schema = new StructType().add("id", "int")
    val jdbc = JDBCRelation(schema, Array.empty, new JDBCOptions(options))(spark)
    assert(render(LogicalRelation(jdbc)) === Some("""spark.read.format("jdbc").load()"""))
    val files = new FileIndex {
      override def rootPaths: Seq[Path] =
        Seq(new Path("s3a://AKIAKEY:s3cr3t@bucket/data"), new Path("/data/2"))
      override def listFiles(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Nil
      override def inputFiles: Array[String] = Array.empty
      override def refresh(): Unit = {}
      override def sizeInBytes: Long = 0L
      override def partitionSchema: StructType = new StructType()
    }
    val parquet = HadoopFsRelation(files, new StructType(), schema, None, new ParquetFileFormat,
      options)(spark)
    assert(render(LogicalRelation(parquet)) ===
      Some("""spark.read.format("parquet").load("s3a://bucket/data", "/data/2")"""))
  }

  test("saveAsTable and insertInto") {
    // Spark 3.5 DataFrameWriter.saveAsTable puts the qualified name of a table of the session
    // catalog into its CreateTable command, the name the user wrote is not kept.
    withTable("big_orders") {
      checkText("""spark.table("orders").filter("amount > 100").write.mode("overwrite")""" +
          """.saveAsTable("default.big_orders")""") {
        orders.filter("amount > 100").write.mode("overwrite").saveAsTable("big_orders")
      }
      checkText("""spark.table("orders").filter("amount < 100").write.insertInto("big_orders")""") {
        orders.filter("amount < 100").write.insertInto("big_orders")
      }
      checkText("""spark.table("orders").write.mode("overwrite").insertInto("big_orders")""") {
        orders.write.mode("overwrite").insertInto("big_orders")
      }
      checkText(
        """spark.table("orders").write.mode("append").saveAsTable("default.big_orders")""") {
        orders.write.mode("append").saveAsTable("big_orders")
      }
    }
    withTable("bucketed") {
      checkText("""spark.table("orders").write.partitionBy("status").bucketBy(2, "order_id")""" +
          """.sortBy("amount").saveAsTable("default.bucketed")""") {
        orders.write.partitionBy("status").bucketBy(2, "order_id").sortBy("amount")
          .saveAsTable("bucketed")
      }
    }
  }

  test("save to a path is shown without options") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      checkText(s"""spark.table("orders").write.format("parquet").mode("append").save("$path")""") {
        orders.write.format("parquet").option("password", "s3cr3t").option("compression", "zstd")
          .mode("append").save(path)
      }
      checkText(s"""spark.table("orders").write.format("json").mode("overwrite").save("$path")""") {
        orders.write.mode("overwrite").json(path)
      }
    }
  }

  test("DataFrameWriterV2 and v2 tables") {
    try {
      checkText("""spark.table("orders").writeTo("testcat.ns.orders")""" +
          """.partitionedBy(expr("status")).create()""") {
        orders.writeTo("testcat.ns.orders").partitionedBy(col("status")).create()
      }
      checkText("""spark.table("orders").filter("amount > 100").writeTo("testcat.ns.orders")""" +
          """.append()""") {
        orders.filter("amount > 100").writeTo("testcat.ns.orders").append()
      }
      checkText("""spark.table("orders").writeTo("testcat.ns.orders").overwrite(expr(""" +
          """"status = 'paid'"))""") {
        orders.writeTo("testcat.ns.orders").overwrite(col("status") === "paid")
      }
      checkText("""spark.table("orders").writeTo("testcat.ns.orders").overwritePartitions()""") {
        orders.writeTo("testcat.ns.orders").overwritePartitions()
      }
      checkText("""spark.table("orders").writeTo("testcat.ns.orders").createOrReplace()""") {
        orders.writeTo("testcat.ns.orders").createOrReplace()
      }
      checkText("""spark.table("orders").write.mode("append").saveAsTable("testcat.ns.orders")""") {
        orders.write.mode("append").saveAsTable("testcat.ns.orders")
      }
      checkText("""spark.table("orders").write.insertInto("testcat.ns.orders")""") {
        orders.write.insertInto("testcat.ns.orders")
      }
      checkText("""spark.table("testcat.ns.orders").filter("amount > 100")""") {
        spark.table("testcat.ns.orders").filter("amount > 100").collect()
      }
    } finally {
      sql("DROP TABLE IF EXISTS testcat.ns.orders")
    }
  }

  test("temporary views") {
    withTempView("big") {
      checkText("""spark.table("orders").filter("amount > 100").createOrReplaceTempView("big")""") {
        orders.filter("amount > 100").createOrReplaceTempView("big")
      }
      checkText("""spark.table("big").select("order_id")""") {
        spark.table("big").select("order_id").collect()
      }
    }
    withGlobalTempView("big_global") {
      checkText("""spark.table("orders").createOrReplaceGlobalTempView("big_global")""") {
        orders.createOrReplaceGlobalTempView("big_global")
      }
    }
  }

  test("views of another session are shown by name only") {
    val other = spark.newSession()
    withGlobalTempView("gv") {
      withGlobalTempView("gv_sql") {
        other.sql("SELECT order_id, amount FROM orders WHERE status <> 'secret_gv'")
          .createOrReplaceGlobalTempView("gv")
        other.sql("CREATE GLOBAL TEMP VIEW gv_sql AS " +
          "SELECT order_id FROM orders WHERE status <> 'secret_gv'")
        checkText("""spark.table("global_temp.gv")""")(spark.table("global_temp.gv").collect())
        checkText("""spark.table("global_temp.gv").groupBy("order_id").count()""") {
          spark.table("global_temp.gv").groupBy("order_id").count().collect()
        }
        checkText("""spark.table("global_temp.gv_sql").filter("order_id > 1")""") {
          spark.table("global_temp.gv_sql").filter("order_id > 1").collect()
        }
      }
    }
  }

  test("temporary views are shown by name only") {
    withTempView("tv") {
      sql("SELECT order_id FROM orders WHERE status <> 'secret_view'")
        .createOrReplaceTempView("tv")
      checkText("""spark.table("tv").filter("order_id > 1")""") {
        spark.table("tv").filter("order_id > 1").collect()
      }
    }
  }

  test("permanent views are shown by name only") {
    withView("pv") {
      sql("CREATE VIEW pv AS SELECT order_id, amount FROM orders WHERE status <> 'secret_view'")
      checkText("""spark.table("pv").filter("amount > 100")""") {
        spark.table("pv").filter("amount > 100").collect()
      }
    }
  }

  test("statements of spark.sql in DataFrame queries") {
    val q = "SELECT customer_id, name FROM customers WHERE city = 'Kazan'"
    checkText(s"""spark.table("orders").join(spark.sql("$q"), Seq("customer_id"), "inner")""") {
      orders.join(spark.sql(q), Seq("customer_id")).collect()
    }
    val quoted = """SELECT 'a "quoted" \ text' AS s"""
    checkText("""spark.range(1).crossJoin(spark.sql("SELECT 'a \"quoted\" \\ text' AS s"))""") {
      spark.range(1).crossJoin(spark.sql(quoted)).collect()
    }
  }

  test("Dataset operations on spark.sql keep the SQL text") {
    val q = "SELECT order_id, amount FROM orders WHERE status = 'paid'"
    assert(seen(spark.sql(q).filter("amount > 100").collect()) === Seq(Some(q) -> None))
    assert(seen(spark.sql(q).show()) === Seq(Some(q) -> None))
    withTable("t") {
      val ctas = "CREATE TABLE t USING parquet AS SELECT * FROM orders"
      assert(seen(sql(ctas)) === Seq(Some(ctas) -> None))
    }
  }

  test("writes of spark.sql statements have the DataFrame text") {
    val q = "SELECT order_id, amount FROM orders WHERE status = 'paid'"
    val statement = s"""spark.sql("$q")"""
    withTable("paid") {
      checkText(s"""$statement.write.mode("overwrite").saveAsTable("default.paid")""") {
        spark.sql(q).write.mode("overwrite").saveAsTable("paid")
      }
      checkText(s"""$statement.write.mode("overwrite").insertInto("paid")""") {
        spark.sql(q).write.mode("overwrite").insertInto("paid")
      }
      checkText(s"""$statement.filter("amount > 100").write.insertInto("paid")""") {
        spark.sql(q).filter("amount > 100").write.insertInto("paid")
      }
    }
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      checkText(s"""$statement.write.format("parquet").save("$path")""") {
        spark.sql(q).write.parquet(path)
      }
      // The statement keeps its text after the write.
      val df = spark.sql(q)
      df.write.mode("overwrite").parquet(path)
      assert(seen(df.collect()) === Seq(Some(q) -> None))
    }
    try {
      checkText(s"""$statement.writeTo("testcat.ns.paid").create()""") {
        spark.sql(q).writeTo("testcat.ns.paid").create()
      }
    } finally {
      sql("DROP TABLE IF EXISTS testcat.ns.paid")
    }
  }

  test("a Dataset keeps its text after its writes") {
    val text = """spark.table("orders").filter("amount > 100")"""
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val collected = orders.filter("amount > 100")
      collected.write.parquet(path)
      assert(collected.queryExecution.tracker.dataFrameText === Some(text))
      checkText(text)(collected.collect())
      val iterated = orders.filter("amount > 100")
      iterated.write.mode("overwrite").parquet(path)
      checkText(text)(iterated.toLocalIterator().hasNext)
    }
    withTable("big") {
      val df = orders.filter("amount > 100")
      df.write.saveAsTable("big")
      checkText(text)(df.collect())
    }
  }

  test("concurrent writes of a Dataset have their own texts") {
    val df = orders.filter("amount > 100")
    withTempPath { dir =>
      val paths = Seq("a", "b").map(name => new java.io.File(dir, name).getCanonicalPath)
      val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]
      val recorded = DataFrameTextRecorder.recordedByThread {
        val threads = paths.map { path =>
          new Thread(s"write to $path") {
            override def run(): Unit = {
              try {
                (1 to 3).foreach(_ => df.write.mode("overwrite").parquet(path))
              } catch {
                case e: Throwable => errors.add(e)
              }
            }
          }
        }
        threads.foreach(_.start())
        threads.foreach(_.join())
      }
      assert(errors.isEmpty, errors)
      paths.foreach { path =>
        val texts =
          recorded.collect { case (thread, _, text) if thread == s"write to $path" => text }
        assert(texts.distinct === Seq(Some("""spark.table("orders").filter("amount > 100")""" +
          s""".write.format("parquet").mode("overwrite").save("$path")""")))
      }
    }
  }

  test("actions are not part of the text") {
    // A new Dataset for every action: the plan of a Dataset is optimized once.
    def df = orders.filter("amount > 100")
    val text = """spark.table("orders").filter("amount > 100")"""
    checkText(text)(df.collect())
    checkText(text)(df.show())
    checkText(text)(df.toLocalIterator().hasNext)
    checkText(text + ".limit(1)")(df.head())
  }

  test("cached Datasets") {
    val df = orders.filter("amount > 100")
    df.cache()
    try {
      checkText("""spark.table("orders").filter("amount > 100").select("order_id")""") {
        df.select("order_id").collect()
      }
    } finally {
      df.unpersist()
    }
  }

  test("the text is redacted with the pattern of the application") {
    val text = """spark.table("orders").filter("status = '*********(redacted)'")"""
    checkText(text)(orders.filter(col("status") === "redact_abc1").collect())
    // Sessions cannot turn the redaction off or redact the whole text.
    Seq("(?!)", "(?s).+").foreach { pattern =>
      withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> pattern) {
        checkText(text)(orders.filter(col("status") === "redact_abc1").collect())
      }
    }
  }

  test("hints, repartition, sample and observe") {
    checkText("""spark.table("orders").join(spark.table("customers").hint("broadcast"), """ +
        """Seq("customer_id"), "inner")""") {
      orders.join(customers.hint("broadcast"), Seq("customer_id")).collect()
    }
    checkText("""spark.table("orders").hint("rebalance", 3)""") {
      orders.hint("rebalance", 3).collect()
    }
    checkText(
      """spark.table("orders").repartition(4, "customer_id").coalesce(2).repartition(3)""") {
      orders.repartition(4, col("customer_id")).coalesce(2).repartition(3).collect()
    }
    checkText("""spark.table("orders").repartitionByRange(2, "amount")""") {
      orders.repartitionByRange(2, col("amount")).collect()
    }
    checkText("""spark.table("orders").sample(0.5)""")(orders.sample(0.5).collect())
    checkText("""spark.table("orders").observe("stats", expr("count(1) AS rows"))""") {
      orders.observe("stats", count(lit(1)).as("rows")).collect()
    }
  }

  test("subqueries in expressions") {
    // Spark 3.5 has no subqueries of Datasets in columns, e.g. Column.isin(Dataset).
    checkText("""spark.table("orders").filter("customer_id IN """ +
        """(SELECT customer_id FROM customers WHERE city = 'Kazan')")""") {
      orders.filter("customer_id IN (SELECT customer_id FROM customers WHERE city = 'Kazan')")
        .collect()
    }
  }

  test("streaming sources") {
    val text = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
      .filter("value > 0").queryExecution.tracker.dataFrameText
    assert(text === Some("""spark.readStream.format("rate").load().filter("value > 0")"""))
  }

  test("row filters and masks of authorization rules are never shown") {
    checkText("""spark.table("secured").filter("id > 0").select("id", "note")""") {
      spark.table("secured").filter("id > 0").select("id", "note").collect()
    }
    checkText("""spark.table("secured").groupBy("status").count()""") {
      spark.table("secured").groupBy("status").count().collect()
    }
    val secured = spark.table("secured")
    checkText("""spark.table("secured").join(spark.table("secured"), Seq("id"), "inner")""") {
      secured.join(secured, Seq("id")).collect()
    }
    checkText("""spark.table("secured").as("a").join(spark.table("secured").as("b"), """ +
        """expr("a.id = b.id"), "inner")""") {
      secured.as("a").join(secured.as("b"), col("a.id") === col("b.id")).collect()
    }
    // Analyzed plans without the tag of the plan they were analyzed from are rendered too.
    val analyzed = spark.table("secured").filter("id > 0").queryExecution.analyzed
    assert(analyzed.nodeName === "DataMaskingStage1Marker")
    assert(render(analyzed.children.head) ===
      Some("""spark.table("secured").filter("id > 0")"""))
    val scan = spark.table("secured").queryExecution.analyzed.children.head
    assert(render(scan) === Some("""spark.table("default.secured")"""))
  }

  test("markers of permanent views are shown by the name of the view") {
    withView("pv") {
      sql("CREATE VIEW pv AS SELECT order_id, amount FROM orders WHERE status <> 'secret_view'")
      val view = spark.table("pv").queryExecution.analyzed
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("pv"))
      val marker = AuthzLikeRules.PermanentViewMarker(view, table)
      assert(render(Project(marker.output, marker)) ===
        Some("""spark.table("default.pv").select("order_id", "amount")"""))
    }
  }

  test("plans built from many Datasets are rendered quickly") {
    val columns = (1 to 200).foldLeft(spark.range(1).toDF()) { (df, i) =>
      df.withColumn(s"c$i", col("id") + i)
    }
    val unions = (1 to 300).map(i => spark.range(1).filter(s"id = $i")).reduce(_ union _)
    val filters = (1 to 500).foldLeft(spark.range(1).toDF())((df, i) => df.filter(col("id") =!= i))
    val start = System.nanoTime()
    val texts = Seq(columns, unions, filters).map(_.queryExecution.tracker.dataFrameText.get)
    assert(System.nanoTime() - start < 5L * 1000 * 1000 * 1000)
    assert(texts(0).startsWith("""spark.range(1).withColumn("c1", expr("id + 1"))"""))
    assert(texts(0).endsWith(""".withColumn("c200", expr("id + 200"))"""))
    assert(""".withColumn\(""".r.findAllMatchIn(texts(0)).size === 200)
    assert(texts(1).startsWith(
      """spark.range(1).filter("id = 1").union(spark.range(1).filter("id = 2"))"""))
    assert(""".union\(""".r.findAllMatchIn(texts(1)).size === 299)
    assert(texts(2).startsWith("""spark.range(1).filter("NOT (id = 1)").filter("NOT (id = 2)")"""))
    assert(""".filter\(""".r.findAllMatchIn(texts(2)).size === 500)
    checkText(texts(2))(filters.collect())
  }

  test("render never throws") {
    assert(render(null) === None)
    val leaf = Range(0, 1, 1, None)
    // A plan that uses the same subquery twice at every level has 2^40 paths.
    val shared = (1 to 40).foldLeft[LogicalPlan](leaf)((p, _) => Union(p, p))
    val text = render(shared).get
    assert(text.startsWith("spark.range(1).union(spark.range(1))"))
    assert(text.length === 100000 + "...".length && text.contains("..."))
    // Too deep to render on the stack.
    val deep = (1 to 100000).foldLeft[LogicalPlan](leaf) { (p, _) =>
      Join(leaf, p, Inner, None, JoinHint.NONE)
    }
    assert(render(deep) === None)
    case class Broken() extends LeafNode {
      override def output: Seq[Attribute] = throw new IllegalStateException("broken")
      override def nodeName: String = throw new IllegalStateException("broken")
    }
    assert(render(Project(Nil, Broken())) === None)
  }

  test("tags of other queries are not replaced") {
    val df = orders.filter("amount > 100")
    val root = df.queryExecution.analyzed
    val tagged = root.getTagValue(DataFrameText.PRE_ANALYSIS_PLAN_TAG)
    assert(tagged.contains(df.queryExecution.logical))
    // The analyzer drops unknown hints, which makes the analyzed plan of the hinted query the
    // analyzed plan of the query it was built from.
    val hinted = df.hint("no_such_hint")
    assert(hinted.queryExecution.analyzed eq root)
    assert(root.getTagValue(DataFrameText.PRE_ANALYSIS_PLAN_TAG) === tagged)
    assert(hinted.queryExecution.tracker.dataFrameText ===
      Some("""spark.table("orders").filter("amount > 100").hint("no_such_hint")"""))
    assert(df.queryExecution.tracker.dataFrameText ===
      Some("""spark.table("orders").filter("amount > 100")"""))
  }

  test("literal values stay in the text") {
    // The SQL parser of Spark 3.5 turns BETWEEN into two comparisons.
    checkText("""spark.table("orders").filter("(status = 'paid') AND """ +
        """((amount >= 1) AND (amount <= 10))")""") {
      orders.filter("status = 'paid' and amount between 1 and 10").collect()
    }
    checkText("""spark.table("orders").withColumn("flag", expr("CASE WHEN (amount > 100) """ +
        """THEN 'big' ELSE 'small' END"))""") {
      orders.withColumn("flag", when(col("amount") > 100, "big").otherwise("small")).collect()
    }
  }

  test("self joins and tables of the same name are shown with what tells them apart") {
    val (big, small) = (orders.filter("amount > 100"), orders.filter("amount < 100"))
    checkText("""spark.table("orders").filter("amount > 100").join(spark.table("orders")""" +
        """.filter("amount < 100"), expr("<left>.order_id = <right>.order_id"), "inner")""") {
      big.join(small, big("order_id") === small("order_id")).collect()
    }
    withDatabase("db2") {
      sql("CREATE DATABASE db2")
      sql("CREATE TABLE db2.orders (order_id INT, note STRING) USING parquet")
      val (default, other) = (spark.table("default.orders"), spark.table("db2.orders"))
      checkText("""spark.table("default.orders").join(spark.table("db2.orders"), """ +
          """expr("default.orders.order_id = db2.orders.order_id"), "inner")""") {
        default.join(other, default("order_id") === other("order_id")).collect()
      }
    }
  }

  test("unpivot") {
    checkText("""spark.table("orders").unpivot(Array(expr("order_id")), """ +
        """Array(expr("customer_id"), expr("amount")), "column", "value")""") {
      orders.unpivot(Array(col("order_id")), Array(col("customer_id"), col("amount")),
        "column", "value").collect()
    }
    checkText("""spark.table("orders").select("order_id", "amount").unpivot(""" +
        """Array(expr("order_id")), "column", "value")""") {
      orders.select("order_id", "amount").unpivot(Array(col("order_id")), "column", "value")
        .collect()
    }
  }

  test("EXISTS, LIKE with ESCAPE and aggregates with FILTER") {
    checkText("""spark.table("orders").filter(""" +
        """"EXISTS(SELECT 1 FROM customers WHERE city = 'Kazan')")""") {
      orders.filter("EXISTS (SELECT 1 FROM customers WHERE city = 'Kazan')").collect()
    }
    checkText("""spark.table("orders").filter("status LIKE 'p!%' ESCAPE '!'")""") {
      orders.filter("status LIKE 'p!%' ESCAPE '!'").collect()
    }
    checkText("""spark.table("orders").groupBy("customer_id").agg(""" +
        """expr("sum(amount) FILTER (WHERE status = 'paid')"))""") {
      orders.groupBy("customer_id").agg(expr("sum(amount) FILTER (WHERE status = 'paid')"))
        .collect()
    }
  }

  test("intersectAll, tail and sample with replacement") {
    checkText("""spark.table("orders").select("customer_id").intersectAll(""" +
        """spark.table("customers").select("customer_id"))""") {
      orders.select("customer_id").intersectAll(customers.select("customer_id")).collect()
    }
    checkText("""spark.table("orders").tail(1)""")(orders.tail(1))
    checkText("""spark.table("orders").sample(true, 0.5)""") {
      orders.sample(true, 0.5).collect()
    }
  }

  test("replace and saveAsTable in ignore mode of v2 tables") {
    try {
      checkText(
        """spark.table("orders").write.mode("ignore").saveAsTable("testcat.ns.ignored")""") {
        orders.write.mode("ignore").saveAsTable("testcat.ns.ignored")
      }
      checkText("""spark.table("orders").filter("amount > 100")""" +
          """.writeTo("testcat.ns.ignored").replace()""") {
        orders.filter("amount > 100").writeTo("testcat.ns.ignored").replace()
      }
    } finally {
      sql("DROP TABLE IF EXISTS testcat.ns.ignored")
    }
  }

  test("other nodes are shown with the plans they read") {
    val customers = ScalarSubquery(UnresolvedRelation(Seq("customers")))
    assert(render(UnknownNodes.Checked(customers, UnresolvedRelation(Seq("orders")))) ===
      Some("""spark.table("orders").Checked(spark.table("customers"))"""))
    assert(render(UnknownNodes.Generated(customers)) ===
      Some("""Generated(spark.table("customers"))"""))
  }

  test("long texts keep their start and their end") {
    val table = UnresolvedRelation(Seq("orders"))
    def write(query: LogicalPlan): LogicalPlan =
      AppendData.byName(UnresolvedRelation(Seq("big")), query)
    val unions = Union((1 to 3000).map { i =>
      Filter(connectFunction("=", UnresolvedAttribute("order_id"), Literal(i)), table)
    })
    val union = render(write(unions)).get
    assert(union.length === 100000 + "...".length && union.contains("..."))
    assert(union.startsWith("""spark.table("orders").filter("order_id = 1").union("""))
    assert(union.endsWith(""".filter("order_id = 3000")).writeTo("big").append()"""))
    val values = (1 to 30000).map(i => Literal(i))
    val in = Filter(connectFunction("in", UnresolvedAttribute("order_id") +: values: _*), table)
    val isin = render(write(in)).get
    assert(isin.length === 100000 + "...".length && isin.contains("..."))
    assert(isin.startsWith("""spark.table("orders").filter("order_id IN (1, 2, """))
    assert(isin.endsWith(""", 30000)").writeTo("big").append()"""))
  }

  test("Spark Connect: columns of DataFrames in joins are shown with their tables") {
    // o.join(c, o("customer_id") === c("customer_id"))
    val (o, c) = (connectTable("orders", 1), connectTable("customers", 2))
    val condition = connectFunction("==", connectColumn("customer_id", 1),
      connectColumn("customer_id", 2))
    val join = withPlanId(Join(o, c, Inner, Some(condition), JoinHint.NONE), 3)
    val joined = """spark.table("orders").join(spark.table("customers"), """ +
      """expr("orders.customer_id = customers.customer_id"), "inner")"""
    assert(render(join) === Some(joined))
    assert(render(Project(Seq(connectColumn("customer_id", 1), connectColumn("name", 2)), join)) ===
      Some(joined + """.select("orders.customer_id", "customers.name")"""))
    assert(render(Filter(connectFunction(">", connectColumn("amount", 1), Literal(100)), join)) ===
      Some(joined + """.filter("orders.amount > 100")"""))
    // The text authorization rules get while the analyzer resolves the columns by plan id.
    checkText(joined)(Dataset.ofRows(spark, join).collect())
  }

  test("Spark Connect: columns of a single DataFrame are shown by name") {
    val big = withPlanId(Filter(connectFunction(">", connectColumn("amount", 1), Literal(100)),
      connectTable("orders", 1)), 2)
    assert(render(big) === Some("""spark.table("orders").filter("amount > 100")"""))
    assert(render(Project(Seq(connectColumn("customer_id", 2)), big)) ===
      Some("""spark.table("orders").filter("amount > 100").select("customer_id")"""))
    // The columns of joined DataFrames can refer to the DataFrames these were built on.
    val names = withPlanId(Project(Seq(UnresolvedAttribute("customer_id"),
      UnresolvedAttribute("name")), connectTable("customers", 3)), 4)
    val condition = connectFunction("==", connectColumn("customer_id", 1),
      connectColumn("customer_id", 4))
    checkText("""spark.table("orders").filter("amount > 100").join(spark.table("customers")""" +
        """.select("customer_id", "name"), expr("orders.customer_id = customers.customer_id"), """ +
        """"inner")""") {
      Dataset.ofRows(spark, Join(big, names, Inner, Some(condition), JoinHint.NONE)).collect()
    }
    // New columns have no table, as in classic DataFrames.
    val totals = withPlanId(Aggregate(Seq(UnresolvedAttribute("customer_id")),
      Seq(UnresolvedAttribute("customer_id"),
        Alias(connectFunction("sum", UnresolvedAttribute("amount")), "total")()),
      connectTable("orders", 5)), 6)
    val totalsJoin = Join(totals, connectTable("customers", 7), Inner,
      Some(connectFunction("==", connectColumn("customer_id", 6), connectColumn("customer_id", 7))),
      JoinHint.NONE)
    checkText("""spark.table("orders").groupBy("customer_id").agg(expr("sum(amount) AS total"))""" +
        """.join(spark.table("customers"), expr("orders.customer_id = customers.customer_id"), """ +
        """"inner").filter("total > 100")""") {
      val filter = Filter(connectFunction(">", connectColumn("total", 6), Literal(100)), totalsJoin)
      Dataset.ofRows(spark, filter).collect()
    }
  }

  test("Spark Connect: self joins with aliases") {
    // a.join(b, a("customer_id") === b("customer_id")) of a = orders.as("a"), b = orders.as("b")
    val a = withPlanId(SubqueryAlias("a", connectTable("orders", 1)), 2)
    val b = withPlanId(SubqueryAlias("b", connectTable("orders", 3)), 4)
    val condition = connectFunction("==", connectColumn("customer_id", 2),
      connectColumn("customer_id", 4))
    checkText("""spark.table("orders").as("a").join(spark.table("orders").as("b"), """ +
        """expr("a.customer_id = b.customer_id"), "inner")""") {
      Dataset.ofRows(spark, Join(a, b, Inner, Some(condition), JoinHint.NONE)).collect()
    }
  }

  test("Spark Connect: self joins without aliases") {
    // a.join(b, a("order_id") === b("order_id")) of a and b, two filters of orders
    val a = withPlanId(Filter(connectFunction(">", connectColumn("amount", 1), Literal(100)),
      connectTable("orders", 1)), 2)
    val b = withPlanId(Filter(connectFunction("<", connectColumn("amount", 3), Literal(100)),
      connectTable("orders", 3)), 4)
    val condition = connectFunction("==", connectColumn("order_id", 2),
      connectColumn("order_id", 4))
    assert(render(Join(a, b, Inner, Some(condition), JoinHint.NONE)) ===
      Some("""spark.table("orders").filter("amount > 100").join(spark.table("orders")""" +
        """.filter("amount < 100"), expr("<left>.order_id = <right>.order_id"), "inner")"""))
  }

  test("Spark Connect: withColumn and withColumns") {
    // The Spark Connect planner calls Dataset.withColumns with metadata, which gives the new
    // columns their names as aliases.
    checkText("""spark.table("orders").withColumn("doubled", expr("amount * 2"))""") {
      orders.withColumns(Seq("doubled"), Seq(col("amount") * 2), Seq(Metadata.empty)).collect()
    }
    checkText("""spark.table("orders").withColumns(Map("one" -> expr("1"), """ +
        """"next" -> expr("amount + 1")))""") {
      orders.withColumns(Seq("one", "next"), Seq(lit(1), col("amount") + 1),
        Seq(Metadata.empty, Metadata.empty)).collect()
    }
    checkText("""spark.table("orders").withColumn("rn", expr("row_number() OVER """ +
        """(PARTITION BY customer_id ORDER BY amount DESC)"))""") {
      val rowNumber =
        row_number().over(Window.partitionBy("customer_id").orderBy(col("amount").desc))
      orders.withColumns(Seq("rn"), Seq(rowNumber), Seq(Metadata.empty)).collect()
    }
    // A column of a joined DataFrame that is unique in the join is shown by name, as in classic.
    val condition = connectFunction("==", connectColumn("customer_id", 1),
      connectColumn("customer_id", 2))
    val join = Join(connectTable("orders", 1), connectTable("customers", 2), Inner,
      Some(condition), JoinHint.NONE)
    checkText("""spark.table("orders").join(spark.table("customers"), """ +
        """expr("orders.customer_id = customers.customer_id"), "inner")""" +
        """.withColumn("doubled", expr("amount * 2"))""") {
      val doubled = connectFunction("*", connectColumn("amount", 1), Literal(2))
      Dataset.ofRows(spark, join)
        .withColumns(Seq("doubled"), Seq(new Column(doubled)), Seq(Metadata.empty))
        .collect()
    }
  }
}

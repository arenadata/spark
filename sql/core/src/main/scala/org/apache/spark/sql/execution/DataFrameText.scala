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

import java.util.{Collections, IdentityHashMap, Locale}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, MultiAlias, MultiInstanceRelation, ResolvedIdentifier, ResolvedStar, UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation, UnresolvedStar, UnresolvedSubqueryColumnAliases}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cube, Descending, Exists, Expression, InSubquery, LambdaFunction, Like, Literal, NamedExpression, NamedLambdaVariable, PrettyAttribute, PythonUDF, RLike, Rollup, ScalaUDF, SortOrder, SubqueryExpression, ToPrettyString, UnresolvedNamedLambdaVariable, UnspecifiedFrame, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, toPrettySQL}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Identifier}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * Builds a readable text of the Dataset API calls a query was made with, e.g.
 * {{{
 *   spark.table("orders").filter("amount > 100").select("customer_id", "amount")
 * }}}
 * Authorization plugins put it into the audit events of queries that have no SQL text.
 *
 * The text shows what the user wrote rather than what the analyzer made of it. Every Dataset
 * operation builds its plan on top of the analyzed plans of other Datasets, and [[QueryExecution]]
 * tags the root of each analyzed plan with the plan it was analyzed from (see
 * [[tagPreAnalysisPlan]]), so rendering goes back to the plans the user built: tables and views
 * appear under the names the user gave, and the row filters, data masks and view markers that
 * authorization rules add during analysis are not shown. Statements run with `spark.sql` appear
 * as `spark.sql("...")`. The text never has option values of data sources (only the paths of
 * files), data of local relations, bodies of views or the functions of typed operations, which
 * are shown as `<local data>`, `<rdd>` and `<function>`. Expressions are shown in SQL syntax with
 * their literals; the caller redacts the whole text.
 *
 * Rendering reads only the given plan and the tags, it never analyzes anything: authorization
 * rules ask for the text while the query is being analyzed.
 */
object DataFrameText {

  /** Set on the root of an analyzed plan: the plan it was analyzed from. */
  val PRE_ANALYSIS_PLAN_TAG: TreeNodeTag[LogicalPlan] = TreeNodeTag[LogicalPlan]("preAnalysisPlan")

  // Longer texts are cut. A query that uses a Dataset several times, e.g. in repeated self joins,
  // can have a text exponentially longer than its plan.
  private val MAX_LENGTH = 100000

  /**
   * Tags `plan`, the result of analyzing `preAnalysis`, with the plan it was analyzed from, unless
   * it already has such a tag or is a node of `preAnalysis` that was analyzed before, e.g. when
   * the analyzer only removed a hint: then the node belongs to another query.
   */
  def tagPreAnalysisPlan(plan: LogicalPlan, preAnalysis: LogicalPlan): Unit = {
    try {
      if (plan.getTagValue(PRE_ANALYSIS_PLAN_TAG).isEmpty && !contains(preAnalysis, plan)) {
        plan.setTagValue(PRE_ANALYSIS_PLAN_TAG, preAnalysis)
      }
    } catch {
      case NonFatal(_) =>
    }
  }

  // Whether `node` is `plan` or one of its nodes, not looking into subtrees analyzed before.
  // Parsed SQL statements can be deep, so this does not recurse.
  private def contains(plan: LogicalPlan, node: LogicalPlan): Boolean = {
    val pending = new java.util.ArrayDeque[LogicalPlan]
    pending.push(plan)
    while (!pending.isEmpty) {
      val next = pending.pop()
      if (next eq node) return true
      if (!next.analyzed) next.children.foreach(pending.push)
    }
    false
  }

  /**
   * Returns the DataFrame-API text of `plan`, the plan a query was built from, or None if it
   * cannot be built. Never throws.
   */
  def render(plan: LogicalPlan): Option[String] = {
    try {
      Option(new Renderer().plan(plan)).filter(_.nonEmpty)
    } catch {
      case NonFatal(_) | _: StackOverflowError => None
    }
  }

  /**
   * Whether the origin of `plan` can be of another query: views and relations can be shared with
   * other queries and sessions and keep the origin of the query that created them, and the nodes
   * of a view body have the view as the object of their origin. Their SQL text is never taken.
   */
  private[sql] def isShared(plan: LogicalPlan): Boolean = plan match {
    case _: View | _: MultiInstanceRelation => true
    case _ => plan.origin.objectType.isDefined
  }

  // What a plan node turns into: a call on the text of a child, no text of its own, or the text
  // that starts the chain of calls.
  private sealed trait Step
  private case class Call(call: String, child: LogicalPlan) extends Step
  private case class Skip(child: LogicalPlan) extends Step
  private case class Source(text: String) extends Step

  private val INFIX_OPERATORS = Map(
    "=" -> "=", "==" -> "=", "<=>" -> "<=>", "!=" -> "!=", "<>" -> "<>", "<" -> "<", "<=" -> "<=",
    ">" -> ">", ">=" -> ">=", "+" -> "+", "-" -> "-", "*" -> "*", "/" -> "/", "%" -> "%",
    "div" -> "div", "&" -> "&", "|" -> "|", "^" -> "^", "and" -> "AND", "or" -> "OR")

  private val PASS_THROUGH_MARKERS = Set("RowFilterMarker", "DataMaskingStage1Marker")

  private class Renderer {
    // Pre-analysis plans being rendered, to stop at cycles of tags.
    private val expanding =
      Collections.newSetFromMap(new IdentityHashMap[LogicalPlan, java.lang.Boolean])
    // A Dataset used several times in a query is rendered once.
    private val rendered = new IdentityHashMap[LogicalPlan, String]
    // The node whose expressions are being rendered, and the lower-case names of the columns that
    // are not unique in its input, None if unknown. Resolved columns are shown with their table
    // only if ambiguous. The names are collected only when a qualified column is rendered: the
    // output of some nodes, e.g. Union in Spark 3, is computed anew on every call.
    private var input: LogicalPlan = null
    private var ambiguous: Option[Set[String]] = null

    // Unary chains, the usual shape of Dataset plans, are walked in a loop, so that long chains
    // of calls do not need a deep stack.
    def plan(root: LogicalPlan): String = {
      val calls = mutable.ArrayBuffer.empty[String]
      val expanded = mutable.ArrayBuffer.empty[LogicalPlan]
      var source: String = null
      var node = root
      try {
        while (source == null) {
          val known = rendered.get(node)
          val preAnalysis = node.getTagValue(PRE_ANALYSIS_PLAN_TAG).orNull
          if (known != null) {
            source = known
          } else if (preAnalysis != null && (preAnalysis ne node) && expanding.add(preAnalysis)) {
            expanded += preAnalysis
            node = preAnalysis
          } else {
            val nodeStep = withInputOf(node) {
              try step(node) catch { case NonFatal(_) => unknown(node) }
            }
            nodeStep match {
              case Call(call, child) =>
                calls += call
                node = child
              case Skip(child) => node = child
              case Source(text) => source = text
            }
          }
        }
      } finally {
        expanded.foreach(expanding.remove)
      }
      val text = cut(calls.reverseIterator.mkString(source, "", ""))
      rendered.put(root, text)
      text
    }

    private def withInputOf[T](p: LogicalPlan)(f: => T): T = {
      val (outerInput, outerAmbiguous) = (input, ambiguous)
      input = p
      ambiguous = null
      try f finally {
        input = outerInput
        ambiguous = outerAmbiguous
      }
    }

    private def isUnique(name: String): Boolean = {
      if (ambiguous == null) {
        ambiguous = try {
          if (input != null && input.childrenResolved) {
            val names = input.children.flatMap(_.output).map(_.name.toLowerCase(Locale.ROOT))
            Some(names.diff(names.distinct).toSet)
          } else {
            None
          }
        } catch {
          case NonFatal(_) => None
        }
      }
      ambiguous.exists(!_.contains(name.toLowerCase(Locale.ROOT)))
    }

    // Spark Connect refers to a column of a DataFrame, e.g. df("id") or df.id in Python, by its
    // name and the id of the plan of the DataFrame. Like a resolved column, it is shown with the
    // alias or the table of that plan if it is in one of several inputs and its name can be
    // ambiguous.
    private def dataFrameColumn(a: UnresolvedAttribute): String = {
      val qualifier = try {
        a.getTagValue(LogicalPlan.PLAN_ID_TAG) match {
          case Some(id) if a.nameParts.size == 1 && input != null && !isUnique(a.nameParts.head) =>
            val name = a.nameParts.head
            planWithId(input, id).flatMap { case (node, inputs, i) =>
              val others = inputs.indices.filter(_ != i).map(j => qualifierOf(inputs(j), name))
              distinctQualifier(qualifierOf(node, name), others, side(inputs, i))
            }
          case _ => None
        }
      } catch {
        case NonFatal(_) => None
      }
      qualifier.map(q => s"$q.${a.name}").getOrElse(a.name)
    }

    // The node with the plan id `id` under `p`, if it is in an input of a node with several
    // children, e.g. on one side of a join: the node, the inputs and the index of its input. Like
    // the analyzer, this does not look into unions.
    private def planWithId(
        p: LogicalPlan,
        id: Long): Option[(LogicalPlan, Seq[LogicalPlan], Int)] = {
      val pending = new java.util.ArrayDeque[(LogicalPlan, Option[(Seq[LogicalPlan], Int)])]
      def addChildren(node: LogicalPlan, branch: Option[(Seq[LogicalPlan], Int)]): Unit = {
        node.children.zipWithIndex.foreach { case (child, i) =>
          pending.add(child -> branch.orElse {
            if (node.children.size > 1) Some(node.children -> i) else None
          })
        }
      }
      addChildren(p, None)
      while (!pending.isEmpty) {
        val (node, branch) = pending.poll()
        if (node.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(id)) {
          return branch.map { case (inputs, i) => (node, inputs, i) }
        }
        if (!node.isInstanceOf[Union]) addChildren(node, branch)
      }
      None
    }

    // The qualifier of the column `name` of a plan: the alias of the plan or the name of the table
    // it reads, looking through the nodes that keep the columns of their child.
    @tailrec
    private def qualifierOf(p: LogicalPlan, name: String): Seq[String] = p match {
      case _ if p.resolved =>
        p.output.find(_.name.equalsIgnoreCase(name)).map(_.qualifier).getOrElse(Nil)
      case s: SubqueryAlias => Seq(s.alias)
      case r: UnresolvedRelation => r.multipartIdentifier
      case project: Project if isAliasOf(project.projectList, name) => Nil
      case a: Aggregate if isAliasOf(a.aggregateExpressions, name) => Nil
      case u: UnaryNode => qualifierOf(u.child, name)
      case _ => Nil
    }

    // What tells a column from the columns of the same name of the other inputs: the shortest end
    // of its qualifier that their qualifiers do not have, e.g. the table, or else the side of the
    // join it comes from, e.g. in a self join of a table without aliases.
    private def distinctQualifier(
        qualifier: Seq[String],
        others: Seq[Seq[String]],
        sideName: => Option[String]): Option[String] = {
      if (qualifier.isEmpty) return None
      (1 to qualifier.size).map(qualifier.takeRight)
        .find(end => !others.exists(_.endsWith(end)))
        .map(_.map(quoteIfNeeded).mkString("."))
        .orElse(sideName)
        .orElse(Some(quoteIfNeeded(qualifier.last)))
    }

    // Shown for the columns of the sides of a join that have the same qualifier.
    private def side(inputs: Seq[LogicalPlan], i: Int): Option[String] = inputs.size match {
      case 2 => Some(if (i == 0) "<left>" else "<right>")
      case _ => None
    }

    // The side of the nearest join under `p` that a resolved column comes from.
    @tailrec
    private def joinSide(p: LogicalPlan, a: Attribute): Option[String] = p match {
      case j: Join => Seq(j.left, j.right).indexWhere(_.outputSet.contains(a)) match {
        case -1 => None
        case i => side(j.children, i)
      }
      case u: UnaryNode => joinSide(u.child, a)
      case _ => None
    }

    // The qualifier of a resolved column whose name is not unique in the input.
    private def resolvedQualifier(a: Attribute): Option[String] = {
      try {
        val others = input.children.flatMap(_.output)
          .filter(o => o.name.equalsIgnoreCase(a.name) && o.exprId != a.exprId).map(_.qualifier)
        distinctQualifier(a.qualifier, others, joinSide(input, a))
      } catch {
        case NonFatal(_) => a.qualifier.lastOption.map(quoteIfNeeded)
      }
    }

    private def isAliasOf(list: Seq[NamedExpression], name: String): Boolean = list.exists {
      case a: Alias => a.name.equalsIgnoreCase(name)
      case _ => false
    }

    private def step(p: LogicalPlan): Step = {
      statementText(p) match {
        case Some(text) => Source(s"spark.sql(${quote(text)})")
        case None => markerStep(p).getOrElse(nodeStep(p))
      }
    }

    // The text of the SQL statement a node was parsed from.
    private def statementText(p: LogicalPlan): Option[String] = {
      if (isShared(p)) None else p.origin.sqlText
    }

    // Nodes added by the authorization rules of Apache Kyuubi: row filters, data masks and their
    // markers are not part of the query the user wrote.
    private def markerStep(p: LogicalPlan): Option[Step] = p match {
      case f: Filter if f.child.nodeName == "RowFilterMarker" => Some(Skip(f.child))
      case _ if PASS_THROUGH_MARKERS.contains(p.nodeName) && p.children.size == 1 =>
        Some(Skip(p.children.head))
      case _ if p.nodeName == "DataMaskingStage0Marker" =>
        // DataMaskingStage0Marker(Project(masks, scan), scan)
        member(p, "scan") match {
          case Some(scan: LogicalPlan) => Some(Skip(scan))
          case _ => p.children.headOption.collect { case masks: Project => Skip(masks.child) }
              .orElse(Some(Source("<masked relation>")))
        }
      case _ => None
    }

    private def nodeStep(p: LogicalPlan): Step = p match {
      case r: UnresolvedRelation =>
        val reader = if (r.isStreaming) "spark.readStream" else "spark"
        Source(s"$reader.table(${quote(r.tableName)})")
      case _ if p.nodeName == "UnresolvedDataSource" => Source(unresolvedDataSource(p))
      case _ if p.nodeName == "StreamingRelation" => Source(streamingRelation(p))
      case s: StreamingRelationV2 => s.identifier match {
        case Some(identifier) =>
          Source(s"spark.readStream.table(${quote(v2Name(s.catalog, identifier))})")
        case None => Source(load(s.sourceName, Nil, streaming = true))
      }
      // Spark 4 names the sources of streaming queries.
      case _ if p.nodeName == "NamedStreamingRelation" && p.children.size == 1 =>
        Skip(p.children.head)
      case _: LocalRelation => Source("spark.createDataFrame(<local data>)")
      case _: LogicalRDD | _: ExternalRDD[_] => Source("spark.createDataFrame(<rdd>)")
      case r: Range => Source(range(r))
      case r: LogicalRelation => logicalRelation(r)
      case r: DataSourceV2Relation if r.identifier.isEmpty =>
        Source(load(v2Format(r), Nil, r.isStreaming))
      case _: View | _: HiveTableRelation | _: DataSourceV2Relation =>
        relationName(p).map(tableSource).getOrElse(unknown(p))

      case f: Filter => Call(s".filter(${quote(top(f.condition))})", f.child)
      case project: Project => projectStep(project)
      case a: Aggregate => aggregate(a)
      case j: Join => join(j)
      case u: Union => Call(u.children.tail.map(unionCall(u, _)).mkString, u.children.head)
      case i: Intersect =>
        Call(s".${if (i.isAll) "intersectAll" else "intersect"}(${plan(i.right)})", i.left)
      case e: Except =>
        Call(s".${if (e.isAll) "exceptAll" else "except"}(${plan(e.right)})", e.left)
      case s: Sort =>
        val method = if (s.global) "orderBy" else "sortWithinPartitions"
        Call(s".$method(${quoteAll(s.order.map(top))})", s.child)
      case l: GlobalLimit => l.child match {
        case local: LocalLimit if local.limitExpr == l.limitExpr => limit(l.limitExpr, local.child)
        case child => limit(l.limitExpr, child)
      }
      case l: LocalLimit => limit(l.limitExpr, l.child)
      case o: Offset => Call(s".offset(${expr(o.offsetExpr)})", o.child)
      case t: Tail => Call(s".tail(${expr(t.limitExpr)})", t.child)
      case d: Deduplicate => deduplicate(d)
      case d: DeduplicateWithinWatermark =>
        Call(s".dropDuplicatesWithinWatermark(${quoteAll(d.keys.map(_.name))})", d.child)
      case d: Distinct => Call(".distinct()", d.child)
      case s: SubqueryAlias =>
        // The analyzer puts the name of a table or a view over its relation.
        if (s.identifier.qualifier.nonEmpty || isRelationAlias(s)) {
          Skip(s.child)
        } else {
          Call(s".as(${quote(s.alias)})", s.child)
        }
      case s: Sample =>
        val fraction = s.upperBound - s.lowerBound
        Call(if (s.withReplacement) s".sample(true, $fraction)" else s".sample($fraction)", s.child)
      case r: Repartition =>
        Call(s".${if (r.shuffle) "repartition" else "coalesce"}(${r.numPartitions})", r.child)
      case r: RepartitionByExpression =>
        val byRange = r.partitionExpressions.nonEmpty &&
          r.partitionExpressions.forall(_.isInstanceOf[SortOrder])
        val args = r.optNumPartitions.map(_.toString).toSeq ++
          r.partitionExpressions.map(e => quote(top(e)))
        Call(s".${if (byRange) "repartitionByRange" else "repartition"}(${args.mkString(", ")})",
          r.child)
      case h: UnresolvedHint =>
        val args = quote(h.name) +: h.parameters.map(hintParameter)
        Call(s".hint(${args.mkString(", ")})", h.child)
      case h: ResolvedHint => h.hints.strategy match {
        case Some(strategy) => Call(s".hint(${quote(strategy.displayName)})", h.child)
        case None => Skip(h.child)
      }
      case u: UnresolvedSubqueryColumnAliases =>
        Call(s".toDF(${quoteAll(u.outputColumnNames)})", u.child)
      case d: DataFrameDropColumns => Call(s".drop(${quoteAll(d.dropList.map(top))})", d.child)
      case c: CollectMetrics =>
        Call(s".observe(${(quote(c.name) +: c.metrics.map(column)).mkString(", ")})", c.child)
      case g: Generate =>
        val output = g.generatorOutput.map(a => quoteIfNeeded(a.name)) match {
          case Seq() => ""
          case Seq(name) => s" AS $name"
          case names => names.mkString(" AS (", ", ", ")")
        }
        Call(s".selectExpr(${quote("*")}, ${quote(top(g.generator) + output)})", g.child)
      case w: Window => Call(w.windowExpressions.map(withColumn).mkString, w.child)
      case pivotNode: Pivot => pivot(pivotNode)
      case u: Unpivot => unpivot(u)
      case j: LateralJoin => lateralJoin(j)
      // Spark 4 only.
      case _ if p.nodeName == "UnresolvedTranspose" && p.children.size == 1 =>
        val indices = member(p, "indices").collect {
          case indices: Seq[_] => indices.collect { case e: Expression => column(e) }
        }.getOrElse(Nil)
        Call(s".transpose(${indices.mkString(", ")})", p.children.head)

      case t: TypedFilter => Call(".filter(<function>)", t.child)
      case m: MapElements => Call(".map(<function>)", m.child)
      case m: MapPartitions => Call(".mapPartitions(<function>)", m.child)
      case a: AppendColumns =>
        val method = if (isAppendColumns(a.child)) "mapValues" else "groupByKey"
        Call(s".$method(<function>)", a.child)
      case m: MapGroups => Call(".mapGroups(<function>)", m.child)
      case f: FlatMapGroupsWithState =>
        val method = if (f.isMapGroupsWithState) "mapGroupsWithState" else "flatMapGroupsWithState"
        Call(s".$method(<function>)", f.child)
      case c: CoGroup => Call(s".cogroup(${plan(c.right)})(<function>)", c.left)
      case s: SerializeFromObject => Skip(s.child)
      case d: DeserializeToObject => Skip(d.child)
      case m: MapInPandas => Call(s".mapInPandas(${pythonFunction(m.functionExpr)})", m.child)
      case f: FlatMapGroupsInPandas =>
        // The Dataset API puts the grouping columns in front of the columns of the grouped data.
        val grouped = resolveTag(f.child) match {
          case columns: Project => columns.child
          case _ => f.child
        }
        val keys = quoteAll(f.groupingAttributes.map(_.name))
        Call(s".groupBy($keys).applyInPandas(${pythonFunction(f.functionExpr)})", grouped)
      case c: CommandResult => Skip(c.commandLogicalPlan)

      case i: InsertIntoStatement =>
        val mode = if (i.overwrite) saveMode(SaveMode.Overwrite, SaveMode.Append) else ""
        Call(s".write$mode.insertInto(${quote(targetName(i.table))})", i.query)
      case a: AppendData => a.table match {
        case r: UnresolvedRelation => Call(s".writeTo(${quote(r.tableName)}).append()", a.query)
        case table => tableWrite(table, a.isByName, SaveMode.Append, a.query)
      }
      case o: OverwriteByExpression => o.table match {
        case r: UnresolvedRelation =>
          val condition = quote(top(o.deleteExpr))
          Call(s".writeTo(${quote(r.tableName)}).overwrite(expr($condition))", o.query)
        case table => tableWrite(table, o.isByName, SaveMode.Overwrite, o.query)
      }
      case o: OverwritePartitionsDynamic => o.table match {
        case r: UnresolvedRelation =>
          Call(s".writeTo(${quote(r.tableName)}).overwritePartitions()", o.query)
        case table => tableWrite(table, o.isByName, SaveMode.Overwrite, o.query)
      }
      case c: CreateTableAsSelect =>
        val name = quote(identifierName(c.name))
        if (c.ignoreIfExists) {
          Call(s""".write.mode("ignore").saveAsTable($name)""", c.query)
        } else {
          Call(s".writeTo($name)${partitionedBy(c.partitioning)}.create()", c.query)
        }
      case r: ReplaceTableAsSelect =>
        val method = if (r.orCreate) "createOrReplace" else "replace"
        val name = quote(identifierName(r.name))
        Call(s".writeTo($name)${partitionedBy(r.partitioning)}.$method()", r.query)
      case i: InsertIntoHadoopFsRelationCommand => i.catalogTable match {
        case Some(table) =>
          val mode = saveMode(i.mode, SaveMode.Append)
          Call(s".write$mode.insertInto(${quote(tableName(table.identifier))})", i.query)
        case None =>
          val path = pathOption(i.options).getOrElse(i.outputPath.toString)
          val format = quote(formatName(i.fileFormat))
          val partitions = partitionBy(i.partitionColumns.map(_.name))
          val save = s".save(${quote(withoutUserInfo(path))})"
          Call(s".write.format($format)${saveMode(i.mode)}$partitions$save", i.query)
      }
      case s: SaveIntoDataSourceCommand =>
        val path = pathOption(s.options).map(p => quote(withoutUserInfo(p))).getOrElse("")
        Call(s".write.format(${quote(formatName(s.dataSource))})${saveMode(s.mode)}.save($path)",
          s.query)
      case c: CreateViewCommand if c.viewType == LocalTempView || c.viewType == GlobalTempView =>
        val kind = if (c.viewType == GlobalTempView) "GlobalTempView" else "TempView"
        val method = if (c.replace) s"createOrReplace$kind" else s"create$kind"
        Call(s".$method(${quote(c.name.table)})", c.plan)
      case m: MergeIntoTable => mergeInto(m)
      case c: CreateTableV1 => saveAsTable(c, Some(c.tableDesc), Some(c.mode), c.query)
      // Spark 4 only.
      case _ if p.nodeName == "SaveAsV1TableCommand" =>
        saveAsTable(p, member(p, "tableDesc").collect { case t: CatalogTable => t },
          member(p, "mode").collect { case m: SaveMode => m },
          member(p, "query").collect { case q: LogicalPlan => q })

      case _ => unknown(p)
    }

    private def isRelationAlias(s: SubqueryAlias): Boolean = {
      val alias = quoteIfNeeded(s.alias).toLowerCase(Locale.ROOT)
      relationName(unwrap(s.child)).map(_.toLowerCase(Locale.ROOT))
        .exists(name => name == alias || name.endsWith("." + alias))
    }

    // Other nodes are shown by name, with the plans they read: their children and the plans of
    // their subqueries.
    private def unknown(p: LogicalPlan): Step = {
      val subqueries = p.expressions.flatMap(_.collect { case s: SubqueryExpression => s.plan })
      (p.children, subqueries) match {
        case (Seq(), Seq()) => relationName(p).map(tableSource).getOrElse(Source(p.nodeName))
        case (Seq(), _) => Source(s"${p.nodeName}(${subqueries.map(plan).mkString(", ")})")
        case (first +: others, _) =>
          Call(s".${p.nodeName}(${(others ++ subqueries).map(plan).mkString(", ")})", first)
      }
    }

    private def unresolvedDataSource(p: LogicalPlan): String = {
      val format = member(p, "format").collect { case f: String => f }.getOrElse(p.nodeName)
      val paths = member(p, "paths").collect {
        case paths: Seq[_] => paths.collect { case path: String => path }
      }.getOrElse(Nil)
      val pathInOptions = member(p, "options").collect {
        case options: scala.collection.Map[_, _] => pathOption(options)
      }.flatten
      load(format, paths ++ pathInOptions, p.isStreaming)
    }

    private def streamingRelation(p: LogicalPlan): String = {
      val format = member(p, "sourceName").collect { case f: String => f }.getOrElse(p.nodeName)
      val dataSource = member(p, "dataSource")
      val paths = dataSource.flatMap(member(_, "paths")).collect {
        case paths: Seq[_] => paths.collect { case path: String => path }
      }.getOrElse(Nil)
      val pathInOptions = dataSource.flatMap(member(_, "options")).collect {
        case options: scala.collection.Map[_, _] => pathOption(options)
      }.flatten
      load(format, paths ++ pathInOptions, streaming = true)
    }

    private def logicalRelation(r: LogicalRelation): Step = r.catalogTable match {
      case Some(table) => tableSource(tableName(table.identifier))
      case None => r.relation match {
        case files: HadoopFsRelation =>
          val paths = files.location.rootPaths.map(_.toString)
          Source(load(formatName(files.fileFormat), paths, r.isStreaming))
        case other => Source(load(relationFormat(other), Nil, r.isStreaming))
      }
    }

    // The number of partitions is left out: SparkSession.range sets it to the default parallelism
    // when the user does not give it.
    private def range(r: Range): String = {
      val args = if (r.step != 1) {
        Seq(r.start, r.end, r.step)
      } else if (r.start != 0) {
        Seq(r.start, r.end)
      } else {
        Seq(r.end)
      }
      s"spark.range(${args.mkString(", ")})"
    }

    private def projectStep(p: Project): Step = {
      val list = p.projectList
      list.headOption.map(_.nodeName) match {
        case _ if isShowProjection(p) => Skip(p.child)
        case Some("UnresolvedStarWithColumns") if list.size == 1 =>
          val names = stringsMember(list.head, "colNames")
          withColumns(names.zip(list.head.children), list.head.children.size).map(Call(_, p.child))
            .getOrElse(Call(s".selectExpr(${quoteAll(list.map(top))})", p.child))
        case Some("UnresolvedStarWithColumnsRenames") if list.size == 1 =>
          val renames = stringsMember(list.head, "existingNames")
            .zip(stringsMember(list.head, "newNames"))
          Call(withColumnsRenamed(renames), p.child)
        case _ if list.forall(isColumn) => Call(s".select(${quoteAll(list.map(expr))})", p.child)
        case _ => resolvedWithColumns(p).map(Call(_, p.child))
          .getOrElse(Call(s".selectExpr(${quoteAll(list.map(top))})", p.child))
      }
    }

    private def withColumns(columns: Seq[(String, Expression)], expected: Int): Option[String] = {
      // Spark Connect sends the new columns with their names as aliases.
      val unaliased = columns.map {
        case (name, a: Alias) if a.name == name => name -> a.child
        case other => other
      }
      unaliased match {
        case Seq() => None
        case _ if unaliased.size != expected => None
        case Seq((name, e)) => Some(s".withColumn(${quote(name)}, ${column(e)})")
        case _ =>
          val map = unaliased.map { case (name, e) => s"${quote(name)} -> ${column(e)}" }
          Some(map.mkString(".withColumns(Map(", ", ", "))"))
      }
    }

    private def withColumnsRenamed(renames: Seq[(String, String)]): String = renames match {
      case Seq((from, to)) => s".withColumnRenamed(${quote(from)}, ${quote(to)})"
      case _ =>
        val map = renames.map { case (from, to) => s"${quote(from)} -> ${quote(to)}" }
        map.mkString(".withColumnsRenamed(Map(", ", ", "))")
    }

    // Spark 3 builds withColumn and withColumnRenamed as a projection of all columns of the
    // analyzed child, some of them replaced or renamed, followed by the new columns.
    private def resolvedWithColumns(p: Project): Option[String] = {
      if (!p.child.resolved || p.projectList.size < p.child.output.size) return None
      val output = p.child.output
      // Dataset.toDF(names) of Spark 3 renames every column.
      val renamesAll = p.projectList.size == output.size && p.projectList.zip(output).forall {
        case (a: Alias, o) => a.child match {
          case c: AttributeReference => c.exprId == o.exprId
          case _ => false
        }
        case _ => false
      }
      if (renamesAll) return Some(s".toDF(${quoteAll(p.projectList.map(_.name))})")
      val added = mutable.ArrayBuffer.empty[(String, Expression)]
      val renamed = mutable.ArrayBuffer.empty[(String, String)]
      var matches = true
      p.projectList.zipWithIndex.foreach {
        case (a: AttributeReference, i) if i < output.size && a.exprId == output(i).exprId =>
        case (a: Alias, i) if i < output.size => a.child match {
          case c: AttributeReference if c.exprId == output(i).exprId && a.name != output(i).name =>
            renamed += output(i).name -> a.name
          case _ if a.name == output(i).name => added += a.name -> a.child
          case _ => matches = false
        }
        case (a: Alias, _) => added += a.name -> a.child
        case _ => matches = false
      }
      if (!matches) {
        None
      } else if (renamed.isEmpty) {
        withColumns(added.toSeq, added.size)
      } else if (added.isEmpty) {
        Some(withColumnsRenamed(renamed.toSeq))
      } else {
        None
      }
    }

    private def aggregate(a: Aggregate): Step = {
      val (method, keys) = a.groupingExpressions match {
        case Seq(r: Rollup) => ("rollup", r.children)
        case Seq(c: Cube) => ("cube", c.children)
        case keys => ("groupBy", keys)
      }
      // The Dataset API puts the grouping columns first in the list of aggregates.
      val retained = a.aggregateExpressions.take(keys.size)
      val aggregates = if (keys.nonEmpty && retained.size == keys.size &&
          retained.zip(keys).forall { case (r, k) => r == k || unalias(r) == k }) {
        a.aggregateExpressions.drop(keys.size)
      } else {
        a.aggregateExpressions
      }
      // Typed aggregations of KeyValueGroupedDataset group by the key of groupByKey.
      val grouping = if (isAppendColumns(a.child) || (keys.isEmpty && method == "groupBy")) {
        ""
      } else if (keys.forall(isColumn)) {
        s".$method(${quoteAll(keys.map(expr))})"
      } else {
        s".$method(${keys.map(column).mkString(", ")})"
      }
      val call = aggregates match {
        case Seq(count: Alias) if count.name == "count" && top(count.child) == "count(1)" =>
          s"$grouping.count()"
        case _ => s"$grouping.agg(${aggregates.map(column).mkString(", ")})"
      }
      Call(call, a.child)
    }

    private def join(j: Join): Step = {
      val right = plan(j.right)
      val call = (j.joinType, j.condition) match {
        case (UsingJoin(joinType, columns), _) =>
          s".join($right, Seq(${quoteAll(columns)}), ${quote(joinTypeName(joinType))})"
        case (Cross, None) => s".crossJoin($right)"
        case (Inner, None) => s".join($right)"
        case (joinType, Some(condition)) =>
          s".join($right, ${column(condition)}, ${quote(joinTypeName(joinType))})"
        case (joinType, None) => s".join($right, ${quote(joinTypeName(joinType))})"
      }
      Call(call, j.left)
    }

    private def lateralJoin(j: LateralJoin): Step = {
      val right = plan(j.right.plan)
      val call = (j.joinType, j.condition) match {
        case (Inner, None) => s".lateralJoin($right)"
        case (joinType, Some(condition)) =>
          s".lateralJoin($right, ${column(condition)}, ${quote(joinTypeName(joinType))})"
        case (joinType, None) => s".lateralJoin($right, ${quote(joinTypeName(joinType))})"
      }
      Call(call, j.left)
    }

    private def joinTypeName(joinType: JoinType): String = joinType match {
      case Inner => "inner"
      case Cross => "cross"
      case LeftOuter => "left"
      case RightOuter => "right"
      case FullOuter => "full"
      case LeftSemi => "left_semi"
      case LeftAnti => "left_anti"
      case other => other.sql.toLowerCase(Locale.ROOT).replace(' ', '_')
    }

    private def unionCall(u: Union, child: LogicalPlan): String = {
      if (!u.byName) {
        s".union(${plan(child)})"
      } else if (u.allowMissingCol) {
        s".unionByName(${plan(child)}, allowMissingColumns = true)"
      } else {
        s".unionByName(${plan(child)})"
      }
    }

    private def limit(n: Expression, child: LogicalPlan): Step = resolveTag(child) match {
      // Dataset.show takes the first rows of a projection that formats every column.
      case p: Project if isShowProjection(p) => Skip(child)
      case _ => Call(s".limit(${expr(n)})", child)
    }

    private def deduplicate(d: Deduplicate): Step = {
      val keys = d.keys.map(_.exprId).toSet
      val all = d.child.resolved && d.child.output.map(_.exprId).toSet == keys
      if (all) {
        Call(".distinct()", d.child)
      } else {
        val names = if (d.child.resolved) {
          d.child.output.filter(a => keys.contains(a.exprId)).map(_.name)
        } else {
          d.keys.map(_.name)
        }
        Call(s".dropDuplicates(${quoteAll(names)})", d.child)
      }
    }

    private def pivot(p: Pivot): Step = {
      val grouping = p.groupByExprsOpt.map(keys => quoteAll(keys.map(top))).getOrElse("")
      val values = if (p.pivotValues.isEmpty) "" else s", Seq(${args(p.pivotValues)})"
      val aggregates = p.aggregates.map(column).mkString(", ")
      Call(s".groupBy($grouping).pivot(${quote(top(p.pivotColumn))}$values).agg($aggregates)",
        p.child)
    }

    private def unpivot(u: Unpivot): Step = {
      def columns(es: Seq[Expression]): String = es.map(column).mkString("Array(", ", ", ")")
      val args = u.ids.map(columns).toSeq ++ u.values.map(values => columns(values.flatten)) ++
        (u.variableColumnName +: u.valueColumnNames).map(quote)
      Call(s".unpivot(${args.mkString(", ")})", u.child)
    }

    private def mergeInto(m: MergeIntoTable): Step = {
      def clause(when: String, action: MergeAction): String = {
        val run = action match {
          case _: DeleteAction => "delete()"
          case _: UpdateStarAction => "updateAll()"
          case _: InsertStarAction => "insertAll()"
          case u: UpdateAction => s"update(${assignments(u.assignments)})"
          case i: InsertAction => s"insert(${assignments(i.assignments)})"
          case other => s"${other.nodeName}()"
        }
        s".$when(${action.condition.map(column).getOrElse("")}).$run"
      }
      val clauses = m.matchedActions.map(clause("whenMatched", _)) ++
        m.notMatchedActions.map(clause("whenNotMatched", _)) ++
        m.notMatchedBySourceActions.map(clause("whenNotMatchedBySource", _))
      // Spark 4 only.
      val evolution =
        if (member(m, "withSchemaEvolution").contains(true)) ".withSchemaEvolution()" else ""
      val target = quote(targetName(m.targetTable))
      Call(s".mergeInto($target, ${column(m.mergeCondition)})${clauses.mkString}$evolution.merge()",
        m.sourceTable)
    }

    private def assignments(list: Seq[Assignment]): String = {
      list.map(a => s"${quote(top(a.key))} -> ${column(a.value)}").mkString("Map(", ", ", ")")
    }

    private def withColumn(e: Expression): String = e match {
      case a: Alias => s".withColumn(${quote(a.name)}, ${column(a.child)})"
      case other => s".withColumn(${quote(top(other))}, ${column(other)})"
    }

    private def tableWrite(
        table: LogicalPlan,
        byName: Boolean,
        mode: SaveMode,
        query: LogicalPlan): Step = {
      relationName(unwrap(table)) match {
        case Some(name) if byName =>
          Call(s".write${saveMode(mode)}.saveAsTable(${quote(name)})", query)
        case Some(name) =>
          Call(s".write${saveMode(mode, SaveMode.Append)}.insertInto(${quote(name)})", query)
        case None =>
          Call(s".write.format(${quote(v2Format(table))})${saveMode(mode)}.save()", query)
      }
    }

    private def saveAsTable(
        p: LogicalPlan,
        table: Option[CatalogTable],
        mode: Option[SaveMode],
        query: Option[LogicalPlan]): Step = {
      (table, query) match {
        case (Some(t), Some(q)) =>
          val name = quote(tableName(t.identifier))
          val layout = partitionBy(t.partitionColumnNames) + t.bucketSpec.map { b =>
            val sortBy =
              if (b.sortColumnNames.isEmpty) "" else s".sortBy(${quoteAll(b.sortColumnNames)})"
            s".bucketBy(${b.numBuckets}, ${quoteAll(b.bucketColumnNames)})$sortBy"
          }.getOrElse("")
          val writeMode = saveMode(mode.getOrElse(SaveMode.ErrorIfExists))
          Call(s".write$writeMode$layout.saveAsTable($name)", q)
        case _ => unknown(p)
      }
    }

    private def isShowProjection(p: Project): Boolean = {
      p.projectList.nonEmpty && p.projectList.forall(e => unalias(e).isInstanceOf[ToPrettyString])
    }

    private def isAppendColumns(p: LogicalPlan): Boolean = resolveTag(p) match {
      case _: AppendColumns => true
      case _ => false
    }

    private def resolveTag(p: LogicalPlan): LogicalPlan = {
      p.getTagValue(PRE_ANALYSIS_PLAN_TAG).filter(_ ne p).getOrElse(p)
    }

    // The relation under the aliases and markers the analyzer and authorization rules add.
    private def unwrap(p: LogicalPlan): LogicalPlan = p match {
      case s: SubqueryAlias => unwrap(s.child)
      case _ => markerStep(p) match {
        case Some(Skip(child)) => unwrap(child)
        case _ => p
      }
    }

    // The name of the table or view a relation reads.
    private def relationName(p: LogicalPlan): Option[String] = p match {
      case r: UnresolvedRelation => Some(r.tableName)
      case v: View => Some(tableName(v.desc.identifier))
      case r: LogicalRelation => r.catalogTable.map(t => tableName(t.identifier))
      case r: HiveTableRelation => Some(tableName(r.tableMeta.identifier))
      case r: DataSourceV2Relation => r.identifier.map(v2Name(r.catalog, _))
      case l: LeafNode =>
        // e.g. the marker of a permanent view added by authorization rules
        member(l, "catalogTable").collect {
          case t: CatalogTable => tableName(t.identifier)
          case Some(t: CatalogTable) => tableName(t.identifier)
        }
      case _ => None
    }

    // The name of the table a command writes to.
    private def targetName(p: LogicalPlan): String = {
      relationName(unwrap(p)).getOrElse(p.nodeName)
    }

    private def identifierName(p: LogicalPlan): String = p match {
      case u: UnresolvedIdentifier => u.nameParts.map(quoteIfNeeded).mkString(".")
      case r: ResolvedIdentifier => v2Name(Some(r.catalog), r.identifier)
      case other => targetName(other)
    }

    private def expr(e: Expression): String = e match {
      case l: Literal => l.sql
      case a: UnresolvedAttribute => dataFrameColumn(a)
      case a: PrettyAttribute => a.name
      case a: Attribute =>
        val name = quoteIfNeeded(a.name)
        if (a.qualifier.isEmpty || isUnique(a.name)) {
          name
        } else {
          resolvedQualifier(a).map(q => s"$q.$name").getOrElse(name)
        }
      case s: UnresolvedStar =>
        s.target.map(t => (t.map(quoteIfNeeded) :+ "*").mkString(".")).getOrElse("*")
      case _: ResolvedStar => "*"
      case _ if e.nodeName == "UnresolvedDataFrameStar" => "*"
      // Spark 4 only: Column.outer(), a column of the outer query of a subquery.
      case _ if e.nodeName == "LazyExpression" && e.children.size == 1 =>
        s"outer(${top(e.children.head)})"
      // Spark 3 names the aggregates of RelationalGroupedDataset after their SQL text.
      case a: Alias if hasGeneratedName(a) => expr(a.child)
      case a: Alias => s"${expr(a.child)} AS ${quoteIfNeeded(a.name)}"
      case a: UnresolvedAlias => expr(a.child)
      case a: MultiAlias => s"${expr(a.child)} AS (${a.names.map(quoteIfNeeded).mkString(", ")})"
      case f: UnresolvedFunction => function(f)
      case u: ScalaUDF => s"${u.udfName.getOrElse("UDF")}(${args(u.children)})"
      case u: PythonUDF => s"${u.name}(${args(u.children)})"
      case s: SortOrder =>
        val direction = if (s.direction == Descending) " DESC" else ""
        val nulls = if (s.nullOrdering != s.direction.defaultNullOrdering) {
          s" ${s.nullOrdering.sql}"
        } else {
          ""
        }
        s"${expr(s.child)}$direction$nulls"
      case w: WindowExpression =>
        val spec = w.windowSpec
        val frame = spec.frameSpecification match {
          case UnspecifiedFrame => ""
          case f => try f.sql catch { case NonFatal(_) => "" }
        }
        val parts = Seq(
          if (spec.partitionSpec.isEmpty) "" else s"PARTITION BY ${args(spec.partitionSpec)}",
          if (spec.orderSpec.isEmpty) "" else s"ORDER BY ${args(spec.orderSpec)}",
          frame).filter(_.nonEmpty)
        s"${expr(w.windowFunction)} OVER (${parts.mkString(" ")})"
      case a: AggregateExpression =>
        val f = a.aggregateFunction
        val call = if (a.isDistinct) s"${f.prettyName}(DISTINCT ${args(f.children)})" else expr(f)
        call + a.filter.map(c => s" FILTER (WHERE ${top(c)})").getOrElse("")
      case l: LambdaFunction =>
        val names = l.arguments.map(expr)
        val params = if (names.size == 1) names.head else names.mkString("(", ", ", ")")
        s"$params -> ${top(l.function)}"
      case v: UnresolvedNamedLambdaVariable => v.name
      case v: NamedLambdaVariable => v.name
      case u: UnresolvedExtractValue => s"${expr(u.child)}[${top(u.extraction)}]"
      case i: InSubquery =>
        val values = if (i.values.size == 1) expr(i.values.head) else s"(${args(i.values)})"
        s"($values IN ${expr(i.query)})"
      case s: SubqueryExpression =>
        // A subquery parsed from an expression, e.g. in Dataset.filter(String), keeps its text.
        val query = sqlFragment(s.plan).getOrElse(plan(s.plan))
        if (s.isInstanceOf[Exists]) s"EXISTS($query)" else s"($query)"
      case l: Like => regex(l.left, "LIKE", l.right, l.escapeChar)
      case r: RLike => regex(r.left, "RLIKE", r.right, '\\')
      case other => generic(other)
    }

    private def function(f: UnresolvedFunction): String = {
      val name = f.nameParts.mkString(".")
      val a = f.arguments
      (name.toLowerCase(Locale.ROOT), a.size) match {
        case (op, 2) if INFIX_OPERATORS.contains(op) =>
          s"(${expr(a.head)} ${INFIX_OPERATORS(op)} ${expr(a(1))})"
        case ("!" | "not", 1) => s"(NOT ${expr(a.head)})"
        case ("-" | "negative", 1) => s"(- ${expr(a.head)})"
        case ("~", 1) => s"~${expr(a.head)}"
        case ("isnull", 1) => s"(${expr(a.head)} IS NULL)"
        case ("isnotnull", 1) => s"(${expr(a.head)} IS NOT NULL)"
        case ("in", n) if n > 0 => s"(${expr(a.head)} IN (${args(a.tail)}))"
        case ("between", 3) => s"(${expr(a.head)} BETWEEN ${expr(a(1))} AND ${expr(a(2))})"
        case (op @ ("like" | "rlike" | "ilike"), 2) =>
          s"(${expr(a.head)} ${op.toUpperCase(Locale.ROOT)} ${expr(a(1))})"
        case _ =>
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          val filter = f.filter.map(c => s" FILTER (WHERE ${top(c)})").getOrElse("")
          s"$name($distinct${args(a)})$filter"
      }
    }

    private def regex(left: Expression, op: String, right: Expression, escape: Char): String = {
      val escapeClause = if (escape == '\\') "" else s" ESCAPE ${Literal(escape.toString).sql}"
      s"(${expr(left)} $op ${expr(right)}$escapeClause)"
    }

    private def hasGeneratedName(a: Alias): Boolean = {
      try a.name == toPrettySQL(a.child) catch { case NonFatal(_) => false }
    }

    // The part of the SQL text a plan was parsed from, if it was parsed outside of views.
    private def sqlFragment(p: LogicalPlan): Option[String] = {
      val origin = p.origin
      for {
        text <- origin.sqlText if origin.objectType.isEmpty
        start <- origin.startIndex
        stop <- origin.stopIndex if start >= 0 && start <= stop && stop < text.length
      } yield text.substring(start, stop + 1)
    }

    // Other expressions print themselves in SQL syntax, with their children rendered here.
    private def generic(e: Expression): String = {
      val children = e.children.map(c => PrettyAttribute(expr(c), dataType(c)))
      try {
        (if (children.isEmpty) e else e.withNewChildren(children)).sql
      } catch {
        case NonFatal(_) => s"${e.prettyName}(${children.map(_.name).mkString(", ")})"
      }
    }

    private def dataType(e: Expression): DataType = {
      try {
        if (e.resolved) e.dataType else NullType
      } catch {
        case NonFatal(_) => NullType
      }
    }

    // An expression without the outer parentheses.
    private def top(e: Expression): String = stripParentheses(expr(e))

    private def args(es: Seq[Expression]): String = es.map(top).mkString(", ")

    private def column(e: Expression): String = s"expr(${quote(top(e))})"

    private def isColumn(e: Expression): Boolean = e match {
      case _: UnresolvedAttribute | _: Attribute | _: UnresolvedStar | _: ResolvedStar => true
      case _ => e.nodeName == "UnresolvedDataFrameStar"
    }

    private def hintParameter(parameter: Any): String = parameter match {
      case l: Literal => l.sql
      case e: Expression => quote(top(e))
      case s: String => quote(s)
      case other => String.valueOf(other)
    }

    private def pythonFunction(e: Expression): String = e match {
      case u: PythonUDF => u.name
      case other => other.prettyName
    }
  }

  private def unalias(e: Expression): Expression = e match {
    case a: Alias => a.child
    case a: UnresolvedAlias => a.child
    case other => other
  }

  private def tableSource(name: String): Source = Source(s"spark.table(${quote(name)})")

  private def load(format: String, paths: Seq[String], streaming: Boolean): String = {
    val reader = if (streaming) "spark.readStream" else "spark.read"
    s"$reader.format(${quote(format)}).load(${quoteAll(paths.map(withoutUserInfo))})"
  }

  private def partitionBy(columns: Seq[String]): String = {
    if (columns.isEmpty) "" else s".partitionBy(${quoteAll(columns)})"
  }

  // DataFrameWriterV2.partitionedBy takes columns and partition transforms like bucket(4, id).
  private def partitionedBy(partitioning: Seq[Transform]): String = {
    if (partitioning.isEmpty) "" else {
      partitioning.map(t => s"expr(${quote(t.describe)})").mkString(".partitionedBy(", ", ", ")")
    }
  }

  private def saveMode(mode: SaveMode, default: SaveMode = SaveMode.ErrorIfExists): String = {
    if (mode == default) "" else s".mode(${quote(mode.name.toLowerCase(Locale.ROOT))})"
  }

  private def tableName(identifier: TableIdentifier): String = {
    (identifier.database.toSeq :+ identifier.table).map(quoteIfNeeded).mkString(".")
  }

  private def v2Name(catalog: Option[CatalogPlugin], identifier: Identifier): String = {
    val catalogName = catalog.map(_.name).filter(_ != CatalogManager.SESSION_CATALOG_NAME)
    (catalogName.toSeq ++ identifier.namespace.toSeq :+ identifier.name)
      .map(quoteIfNeeded).mkString(".")
  }

  private def v2Format(p: LogicalPlan): String = p match {
    case r: DataSourceV2Relation =>
      r.table.getClass.getSimpleName.stripSuffix("Table").toLowerCase(Locale.ROOT)
    case other => other.nodeName
  }

  private def formatName(format: AnyRef): String = format match {
    case r: DataSourceRegister => r.shortName()
    case other => other.getClass.getSimpleName
  }

  private def relationFormat(relation: BaseRelation): String = {
    relation.getClass.getSimpleName.stripSuffix("Relation").toLowerCase(Locale.ROOT)
  }

  // Data source options are never shown, except for the path of the data.
  private def pathOption(options: scala.collection.Map[_, _]): Option[String] = {
    options.collectFirst { case (k: String, v: String) if k.equalsIgnoreCase("path") => v }
  }

  private val USER_INFO = "^([A-Za-z][A-Za-z0-9+.-]*://)[^/?#]*@".r

  // Paths can have credentials in their user info, e.g. s3a://key:secret@bucket/path.
  private def withoutUserInfo(path: String): String = USER_INFO.replaceFirstIn(path, "$1")

  private def member(obj: Any, name: String): Option[Any] = {
    try {
      Option(obj.getClass.getMethod(name).invoke(obj))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def stringsMember(obj: Any, name: String): Seq[String] = member(obj, name).collect {
    case values: Seq[_] => values.collect { case s: String => s }
  }.getOrElse(Nil)

  private def quote(s: String): String = {
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  }

  private def quoteAll(strings: Seq[String]): String = strings.map(quote).mkString(", ")

  // Longer texts keep their start, which shows where the data comes from, and their end, which
  // shows the last calls, e.g. the write.
  private def cut(text: String): String = {
    if (text.length > MAX_LENGTH) {
      text.substring(0, MAX_LENGTH / 2) + "..." + text.substring(text.length - MAX_LENGTH / 2)
    } else {
      text
    }
  }

  // Removes the parentheses around the whole text, if any.
  private def stripParentheses(s: String): String = {
    if (s.length > 1 && s.charAt(0) == '(' && closingParenthesis(s) == s.length - 1) {
      s.substring(1, s.length - 1)
    } else {
      s
    }
  }

  // The index of the parenthesis that closes the one the text starts with, -1 if none.
  private def closingParenthesis(s: String): Int = {
    var depth = 0
    var quoteChar = 0.toChar
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (quoteChar != 0) {
        if (c == '\\') {
          i += 1
        } else if (c == quoteChar) {
          quoteChar = 0
        }
      } else if (c == '\'' || c == '"' || c == '`') {
        quoteChar = c
      } else if (c == '(') {
        depth += 1
      } else if (c == ')') {
        depth -= 1
        if (depth == 0) return i
      }
      i += 1
    }
    -1
  }
}

/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.memcypher.impl.physical

import org.opencypher.memcypher.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.memcypher.impl.flat
import org.opencypher.memcypher.impl.flat.FlatOperator
import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.Orientation.Directed
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl._

class PhysicalPlanner[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]](producer: PhysicalOperatorProducer[P, R, G, C])

  extends DirectCompilationStage[FlatOperator, P, PhysicalPlannerContext[P, R]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[P, R]): P = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        producer.planCartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        producer.planRemoveAliases(process(in), dependent, header)

      case flat.Select(fields, in, header) =>

        val selectExpressions = fields
          //ToDo check how to replace selfwithChildren -> see FlatOperatorProduced
          .flatMap(header.selfWithChildren)
          .map(_.content.key)
          .distinct

        producer.planSelect(process(in), selectExpressions, header)

      case flat.EmptyRecords(in, header) =>
        producer.planEmptyRecords(process(in), header)

      //ToDO fix once the flat.Start works
      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planStart(Some(g.qualifiedGraphName), Some(context.inputRecords))
          case p: LogicalPatternGraph =>
            context.constructedGraphPlans.get(p.qualifiedGraphName) match {
              case Some(plan) => plan // the graph was already constructed
              case None => planConstructGraph(None, p) // plan starts with a construct graph, thus we have to plan it
            }
        }

      case flat.FromGraph(graph, in) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planFromGraph(process(in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct)
        }

      case op
        @flat.NodeScan(v, in, header) => producer.planNodeScan(process(in), op.sourceGraph, v, header)

      case op
        @flat.EdgeScan(e, in, header) => producer.planRelationshipScan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) => producer.planAlias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header) =>
        val explodeExpr = Explode(list)(item.cypherType)
        //ToDo see FlatOperatorProducer
        val withExplodedHeader = in.header.update(addContent(ProjectedExpr(explodeExpr)))._1
        val withExploded = producer.planProject(process(in), explodeExpr, withExplodedHeader)
        producer.planAlias(withExploded, explodeExpr, item, header)

      case flat.Project(expr, in, header) => producer.planProject(process(in), expr, header)

      case flat.Aggregate(aggregations, group, in, header) => producer.planAggregate(process(in), group, aggregations, header)

      case flat.Filter(expr, in, header) => expr match {
        //ToDO check why TrueLit cant be imported -> object/case class problems?
        case TrueLit() =>
          process(in) // optimise away filter
        case _ =>
          producer.planFilter(process(in), expr, header)
      }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions, header)

      case flat.Distinct(fields, in, _) =>
        producer.planDistinct(process(in), fields)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val startFrom = sourceOp.sourceGraph match {
          case e: LogicalCatalogGraph =>
            producer.planStart(Some(e.qualifiedGraphName))

          case c: LogicalPatternGraph =>
            context.constructedGraphPlans(c.qualifiedGraphName)
        }

        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel, relHeader)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            producer.planJoin(tempResult, third, Seq(endNode -> target), header)

          case Undirected =>
            val tempOutgoing = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            val outgoing = producer.planJoin(tempOutgoing, third, Seq(endNode -> target), header)

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = producer.planFilter(second, filterExpression, second.header)

            val tempIncoming = producer.planJoin(third, relsWithoutLoops, Seq(target -> startNode), third.header ++ second.header)
            val incoming = producer.planJoin(tempIncoming, first, Seq(endNode -> source), header)

            producer.planTabularUnionAll(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel, relHeader)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode), header)
            producer.planTabularUnionAll(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(rel, edgeList, target, direction, lower, upper, sourceOp, relOp, targetOp, header, isExpandInto) =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        producer.planBoundedVarExpand(
          first,
          second,
          third,
          rel,
          edgeList,
          target,
          sourceOp.endNode,
          lower,
          upper,
          direction,
          header,
          isExpandInto)

      case flat.Optional(lhs, rhs, header) => planOptional(lhs, rhs, header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) => producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header) => producer.planLimit(process(in), expr, header)

      case flat.ReturnGraph(in) => producer.planReturnGraph(process(in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  private def planConstructGraph(in: Option[FlatOperator], construct: LogicalPatternGraph)(implicit context: PhysicalPlannerContext[P, R]) = {
    val onGraphPlan = {
      construct.onGraphs match {
        case Nil => producer.planStart() // Empty start
        //TODO: Optimize case where no union is necessary
        //case h :: Nil => producer.planStart(Some(h)) // Just one graph, no union required
        case several =>
          val onGraphPlans = several.map(qgn => producer.planStart(Some(qgn)))
          producer.planGraphUnionAll(onGraphPlans, construct.qualifiedGraphName)
      }
    }
    val inputTablePlan = in.map(process).getOrElse(producer.planStart())

    val constructGraphPlan = producer.planConstructGraph(inputTablePlan, onGraphPlan, construct)
    context.constructedGraphPlans.update(construct.qualifiedGraphName, constructGraphPlan)
    constructGraphPlan
  }

  private def planOptional(lhs: FlatOperator, rhs: FlatOperator, header: RecordHeader)(implicit context: PhysicalPlannerContext[P, R]) = {
    val lhsData = process(lhs)
    val rhsData = process(rhs)
    val lhsHeader = lhs.header
    val rhsHeader = rhs.header

    // 1. Compute fields between left and right side
    //val commonFields = lhsHeader.slots.map(_.content).intersect(rhsHeader.slots.map(_.content))
    val commonFields = lhsHeader.expressions.intersect(rhsHeader.expressions)

    val (joinSlots, otherCommonSlots) = commonFields.partition {
      //ToDo check how to replace Opaquefield
      case _: OpaqueField => true
      case _ => false
    }

    val joinFields = joinSlots
      .collect { case OpaqueField(v) => v }
      .distinct

    val otherCommonFields = otherCommonSlots
      .map(_.key)

    // 2. Remove siblings of the join fields and other common fields
    val fieldsToRemove = joinFields
      //ToDo check how to replace childSlots
      .flatMap(rhsHeader.childSlots)
      .map(_.content.key)
      .union(otherCommonFields)
      .distinct

    val rhsHeaderWithDropped = fieldsToRemove.flatMap(rhsHeader.slotsFor).foldLeft(rhsHeader)(_ - _)
    val rhsWithDropped = producer.planDrop(rhsData, fieldsToRemove, rhsHeaderWithDropped)

    // 3. Rename the join fields on the right hand side, in order to make them distinguishable after the join
    //ToDo generateUniqueName can probably be replaced by generateConflictFreeName
    val joinFieldRenames = joinFields.map(f => f -> Var(rhsHeader.generateUniqueName)(f.cypherType)).toMap

    val rhsWithRenamedSlots = rhsHeaderWithDropped.slots.collect {
      case RecordSlot(i, OpaqueField(v)) if joinFieldRenames.contains(v) => RecordSlot(i, OpaqueField(joinFieldRenames(v)))
      case other => other
    }
    val rhsHeaderWithRenamed = RecordHeader.from(rhsWithRenamedSlots.toList)
    val rhsWithRenamed = producer.planAlias(rhsWithDropped, joinFieldRenames.toSeq, rhsHeaderWithRenamed)

    // 4. Left outer join the left side and the processed right side
    val joined = producer.planJoin(lhsData, rhsWithRenamed, joinFieldRenames.toSeq, lhsHeader ++ rhsHeaderWithRenamed, LeftOuterJoin)

    // 5. Drop the duplicate join fields
    producer.planDrop(joined, joinFieldRenames.values.toSeq, header)
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}

/*
class PhysicalPlanner[
  O <: FlatRelationalTable[O],
  K <: PhysicalOperator[O, A, P, I],
  A <: RelationalCypherRecords[O],
  P <: PropertyGraph,
  I <: RuntimeContext[O, A, P]](val producer: PhysicalOperatorProducer[O, K, A, P, I])
  extends DirectCompilationStage[FlatOperator, K, PhysicalPlannerContext[O, K, A]] {

  private implicit val planner: PhysicalPlanner[O, K, A, P, I] = this

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[O, K, A]): K = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs) =>
        producer.planCartesianProduct(process(lhs), process(rhs))

      case flat.Select(fields, in) =>

        val inOp = process(in)

        val selectExpressions = fields
          .flatMap(inOp.header.ownedBy)
          .distinct

        producer.planSelect(inOp, selectExpressions)

      case flat.Project(projectExpr, in) =>
        val inOp = process(in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.header.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => producer.planAlias(inOp, expr as alias)
          case Some(alias) => producer.planAdd(inOp, expr as alias)
          case None => producer.planAdd(inOp, expr)
        }

      case flat.EmptyRecords(in, fields) =>
        producer.planEmptyRecords(process(in), fields)

      case flat.Start(graph) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planStart(Some(g.qualifiedGraphName), Some(context.inputRecords))
          case p: LogicalPatternGraph =>
            context.constructedGraphPlans.get(p.name) match {
              case Some(plan) => plan // the graph was already constructed
              // TODO: investigate why the implicit context is not found in scope
              case None => planConstructGraph(None, p)(context, planner) // plan starts with a construct graph, thus we have to plan it
            }
        }

      case flat.FromGraph(graph, in) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planFromGraph(process(in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct)(context, planner)
        }

      case op
        @flat.NodeScan(v, in) => producer.planNodeScan(process(in), op.sourceGraph, v)

      case op
        @flat.RelationshipScan(e, in) => producer.planRelationshipScan(process(in), op.sourceGraph, e)

      case flat.Alias(expr, in) => producer.planAlias(process(in), expr)

      case flat.WithColumn(expr, in) =>
        producer.planAdd(process(in), expr)

      case flat.Aggregate(aggregations, group, in) => producer.planAggregate(process(in), group, aggregations)

      case flat.Filter(expr, in) => expr match {
        case TrueLit =>
          process(in) // optimise away filter
        case _ =>
          producer.planFilter(process(in), expr)
      }

      case flat.ValueJoin(lhs, rhs, predicates) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions)

      case flat.Distinct(fields, in) =>
        val entityExprs: Set[Var] = Set(fields.toSeq: _*)
        producer.planDistinct(process(in), entityExprs)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val startFrom = sourceOp.sourceGraph match {
          case e: LogicalCatalogGraph =>
            producer.planStart(Some(e.qualifiedGraphName))

          case c: LogicalPatternGraph =>
            context.constructedGraphPlans(c.qualifiedGraphName)
        }

        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = producer.planJoin(first, second, Seq(source -> startNode))
            producer.planJoin(tempResult, third, Seq(endNode -> target))

          case Undirected =>
            val tempOutgoing = producer.planJoin(first, second, Seq(source -> startNode))
            val outgoing = producer.planJoin(tempOutgoing, third, Seq(endNode -> target))

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = producer.planFilter(second, filterExpression)

            val tempIncoming = producer.planJoin(third, relsWithoutLoops, Seq(target -> startNode))
            val incoming = producer.planJoin(tempIncoming, first, Seq(endNode -> source))

            producer.planTabularUnionAll(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode))

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode))
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode))
            producer.planTabularUnionAll(outgoing, incoming)
        }

      case flat.BoundedVarExpand(
      source, list, edgeScan, target,
      direction, lower, upper,
      sourceOp, edgeScanOp, targetOp,
      isExpandInto
      ) =>
        val planner = direction match {
          case Directed => new DirectedVarLengthExpandPlanner[O, K, A, P, I](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)(this, context)

          case Undirected => new UndirectedVarLengthExpandPlanner[O, K, A, P, I](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)(this, context)
        }

        planner.plan

      case flat.Optional(lhs, rhs) => planOptional(lhs, rhs)

      case flat.ExistsSubQuery(predicateField, lhs, rhs) =>

        val leftResult = process(lhs)
        val rightResult = process(rhs)

        val leftHeader = leftResult.header
        val rightHeader = rightResult.header

        // 0. Find common expressions, i.e. join expressions
        val joinExprs = leftHeader.vars.intersect(rightHeader.vars)
        // 1. Alias join expressions on rhs
        val renameExprs = joinExprs.map(e => e as Var(s"${e.name}${System.nanoTime}")(e.cypherType))
        val rightWithAliases = producer.planAliases(rightResult, renameExprs.toSeq)
        // 2. Drop Join expressions and their children in rhs
        val epxrsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = producer.planDrop(rightWithAliases, epxrsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = producer.planDistinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = producer.planJoin(leftResult, distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        producer.planAddInto(joinedData, IsNotNull(targetExpr)(CTBoolean), predicateField)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in) =>
        producer.planOrderBy(process(in), sortItems)

      case flat.Skip(expr, in) => producer.planSkip(process(in), expr)

      case flat.Limit(expr, in) => producer.planLimit(process(in), expr)

      case flat.ReturnGraph(in) => producer.planReturnGraph(process(in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }
  //
  //  private def planConstructGraph(in: Option[FlatOperator], construct: LogicalPatternGraph)
  //    (implicit context: PhysicalPlannerContext[O, K, A]) = {
  //    val onGraphPlan = {
  //      construct.onGraphs match {
  //        case Nil => producer.planStart() // Empty start
  //        //TODO: Optimize case where no union is necessary
  //        //case h :: Nil => producer.planStart(Some(h)) // Just one graph, no union required
  //        case several =>
  //          val onGraphPlans = several.map(qgn => producer.planStart(Some(qgn)))
  //          producer.planGraphUnionAll(onGraphPlans, construct.name)
  //      }
  //    }
  //    val inputTablePlan = in.map(process).getOrElse(producer.planStart())
  //
  //    val constructGraphPlan = producer.planConstructGraph(inputTablePlan, onGraphPlan, construct)
  //    context.constructedGraphPlans.update(construct.name, constructGraphPlan)
  //    constructGraphPlan
  //  }

  private def planOptional(lhs: FlatOperator, rhs: FlatOperator)
                          (implicit context: PhysicalPlannerContext[O, K, A]) = {
    val lhsOp = process(lhs)
    val rhsOp = process(rhs)

    val lhsHeader = lhsOp.header
    val rhsHeader = rhsOp.header

    def generateUniqueName = s"tmp${System.nanoTime}"

    // 1. Compute expressions between left and right side
    val commonExpressions = lhsHeader.expressions.intersect(rhsHeader.expressions)
    val joinExprs = commonExpressions.collect { case v: Var => v }
    val otherExpressions = commonExpressions -- joinExprs

    // 2. Remove siblings of the join expressions and other common fields
    val expressionsToRemove = joinExprs
      .flatMap(v => rhsHeader.ownedBy(v) - v)
      .union(otherExpressions)
    val rhsWithDropped = producer.planDrop(rhsOp, expressionsToRemove)

    // 3. Rename the join expressions on the right hand side, in order to make them distinguishable after the join
    val joinExprRenames = joinExprs.map(e => e as Var(generateUniqueName)(e.cypherType))
    val rhsWithAlias = producer.planAliases(rhsWithDropped, joinExprRenames.toSeq)
    val rhsJoinReady = producer.planDrop(rhsWithAlias, joinExprs.collect { case e: Expr => e })

    // 4. Left outer join the left side and the processed right side
    val joined = producer.planJoin(lhsOp, rhsJoinReady, joinExprRenames.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)

    // 5. Select the resulting header expressions
    producer.planSelect(joined, joined.header.expressions.toList)
  }
}

*/


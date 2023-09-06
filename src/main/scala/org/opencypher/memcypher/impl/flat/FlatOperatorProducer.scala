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
package org.opencypher.memcypher.impl.flat

import cats.Monoid
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalStateException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer
import org.opencypher.okapi.logical.impl.{Direction, LogicalGraph}
import org.opencypher.okapi.relational.api.schema.RelationalSchema
import org.opencypher.okapi.relational.impl.exception.RecordHeaderException
import org.opencypher.okapi.relational.impl.table._

import scala.annotation.tailrec

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private implicit val typeVectorMonoid: Monoid[Vector[CypherType]] {
    def empty: Vector[CypherType]

    def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType]
  } = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def cartesianProduct(lhs: FlatOperator, rhs: FlatOperator): CartesianProduct = {
    val header = lhs.header ++ rhs.header
    CartesianProduct(lhs, rhs, header)
  }

  def select(fields: List[Var], in: FlatOperator): Select = {
    val fieldContents = fields.map { field =>
      in.header.slotFor(field).content
    }

    //ToDo check if childSlots can be replaced by ownedby? -> both need an expression, but somehow it isnt required here for childSlots?
    val finalContents = fieldContents ++ fields.flatMap(in.header.childSlots).map(_.content)

    //val (nextHeader, _) = RecordHeader.empty.update(addContents(finalContents))
    //ToDo check if this works once the finalContents is fixed
    val (nextHeader, _) = RecordHeader.from(finalContents)

    Select(fields, in, nextHeader)
  }

  def returnGraph(in: FlatOperator): ReturnGraph = {
    ReturnGraph(in)
  }

  def removeAliases(toKeep: List[Var], in: FlatOperator): FlatOperator = {
    //val renames = in.header.contents.collect {
    //ToDo RemoveAliases requires an AliasExpr, change the collection to get an AliasExpr
    val renames = in.header.expressions.collect {
      //case pf @ ProjectedField(v, _: Property | _: HasLabel | _: HasType) if !toKeep.contains(v) =>
      case pf @ ProjectedField(v, _: Property | _: HasLabel | _: HasType) if !toKeep.contains(v) =>
        pf -> ProjectedExpr(pf.expr)
    }

    if (renames.isEmpty) {
      in
    } else {
      //val newHeaderContents = in.header.contents.map {
      val newHeaderContents = in.header.expressions.map {
        case pf @ ProjectedField(v, _: Property | _: HasLabel | _: HasType) if !toKeep.contains(v) =>
          ProjectedExpr(pf.expr)
        case other =>
          other
      }

      //ToDo see line 69
      val (header, _) = RecordHeader.empty.update(addContents(newHeaderContents.toSeq))

      RemoveAliases(renames, in, header)
    }
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    in.header

    //    expr match {
    //      case HasLabel(n, label) =>
    //        in.header.contents.map { c =>
    //
    //        }
    //      case _ => in.header
    //    }

    // TODO: Should replace SlotContent expressions with detailed type of entity
    // TODO: Should reduce width of header due to more label information

    Filter(expr, in, in.header)
  }

  def distinct(fields: Set[Var], in: FlatOperator): Distinct = {
    Distinct(fields, in, in.header)
  }

  /**
   * This acts like a leaf operator even though it has an ancestor in the tree.
   * That means that it will discard any incoming fields from the ancestor header (assumes it is empty)
   */
  def nodeScan(node: Var, prev: FlatOperator): NodeScan = {
    //val header = RecordHeader.nodeFromSchema(node, prev.sourceGraph.schema)
    val header = RelationalSchema.SchemaOps(prev.sourceGraph.schema).headerForNode(node)

    new NodeScan(node, prev, header)
  }

  def edgeScan(edge: Var, prev: FlatOperator): EdgeScan = {
    //val edgeHeader = RecordHeader.relationshipFromSchema(edge, prev.sourceGraph.schema)
    val edgeHeader = RelationalSchema.SchemaOps(prev.sourceGraph.schema).headerForRelationship(edge)

    EdgeScan(edge, prev, edgeHeader)
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = {
    t match {
      case l: CTList         => relTypeFromList(l.elementType)
      case r: CTRelationship => r.types
      case _                 => throw IllegalStateException(s"Required CTList or CTRelationship, but got $t")
    }
  }

  def varLengthEdgeScan(edgeList: Var, prev: FlatOperator): EdgeScan = {
    val types = relTypeFromList(edgeList.cypherType)
    val edge = FreshVariableNamer(edgeList.name + "extended", CTRelationship(types, edgeList.cypherType.graph))
    edgeScan(edge, prev)
  }


  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    //ToDo see line 69 for RecordHeader.empty.update
    val (newHeader, _) = RecordHeader.empty.update(
      addContents(
        //ToDo old selfwithChildren used: slotFor(expr) + childSlots(expr)
        group.flatMap(in.header.selfWithChildren).map(_.content).toSeq ++ aggregations.map(agg => OpaqueField(agg._1)))
    )

    Aggregate(aggregations, group, in, newHeader)
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): Unwind = {
    //ToDO replace RecordHeaderSyntax.update -> this class no longer exists?
    val (header, _) = in.header.update(addContent(OpaqueField(item)))

    Unwind(list, item, in, header)
  }

  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    //ToDo see line 172
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      //ToDo relational.impl.table.UpdateResult class no longer exists
      case _: Found[_]       => in
      case _: Replaced[_]    => Alias(it.expr, it.alias.get, in, newHeader)
      case _: Added[_]       => Project(it.expr, in, newHeader)
      case f: FailedToAdd[_] => throw RecordHeaderException(s"Slot already exists: ${f.conflict}")
    }
  }

  def expand(
              source: Var,
              rel: Var,
              direction: Direction,
              target: Var,
              schema: Schema,
              sourceOp: FlatOperator,
              targetOp: FlatOperator): FlatOperator = {
    //val relHeader = RecordHeader.relationshipFromSchema(rel, schema)
    val relHeader = RelationalSchema.SchemaOps(schema).headerForRelationship(rel)

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    Expand(source, rel, direction, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def expandInto(source: Var, rel: Var, target: Var, direction: Direction, schema: Schema, sourceOp: FlatOperator): FlatOperator = {
    //val relHeader = RecordHeader.relationshipFromSchema(rel, schema)
    val relHeader = RelationalSchema.SchemaOps(schema).headerForRelationship(rel)

    val expandHeader = sourceOp.header ++ relHeader

    ExpandInto(source, rel, target, direction, sourceOp, expandHeader, relHeader)
  }

  def valueJoin(
                 lhs: FlatOperator,
                 rhs: FlatOperator,
                 predicates: Set[org.opencypher.okapi.ir.api.expr.Equals]): FlatOperator = {
    ValueJoin(lhs, rhs, predicates, lhs.header ++ rhs.header)
  }

  def planFromGraph(graph: LogicalGraph, prev: FlatOperator) = {
    FromGraph(graph, prev)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    val header = RecordHeader.from(fields.map(OpaqueField).toSeq: _*)
    EmptyRecords(prev, header)
  }

  //def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
  def planStart(graph: LogicalGraph): Start = {
    //Start(graph, fields)
    Start(graph)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    //ToDo line 172
    val (header, _) = in.header.update(addContents(Seq(OpaqueField(edgeList), OpaqueField(endNodeId))))

    InitVarExpand(source, edgeList, endNodeId, in, header)
  }

  def boundedVarExpand(
                        edge: Var,
                        edgeList: Var,
                        target: Var,
                        direction: Direction,
                        lower: Int,
                        upper: Int,
                        sourceOp: InitVarExpand,
                        edgeOp: FlatOperator,
                        targetOp: FlatOperator,
                        isExpandInto: Boolean): FlatOperator = {

    //ToDo line 172
    val (initHeader, _) = sourceOp.in.header.update(addContent(OpaqueField(edgeList)))
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, direction, lower, upper, sourceOp, edgeOp, targetOp, header, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, rhs.header)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    //ToDo line 172
    val (header, status) = lhs.header.update(addContent(ProjectedField(expr.targetField, expr)))

    //old todo TODO: record header should have sealed effects or be a map
    //ToDo relational.impl.table.updateresult no longer exists
    status match {
      case _: Added[_]       => ExistsSubQuery(expr.targetField, lhs, rhs, header)
      case f: FailedToAdd[_] => throw RecordHeaderException(s"Slot already exists: ${f.conflict}")
      case _                 => throw RecordHeaderException("Invalid RecordHeader update status.")
    }
  }

  def orderBy(sortItems: Seq[SortItem], sourceOp: FlatOperator): FlatOperator = {
    OrderBy(sortItems, sourceOp, sourceOp.header)
  }

  def skip(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Skip(expr, sourceOp, sourceOp.header)
  }

  def limit(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Limit(expr, sourceOp, sourceOp.header)
  }
}

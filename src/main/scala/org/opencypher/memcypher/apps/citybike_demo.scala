package org.opencypher.memcypher.apps

import com.typesafe.scalalogging.Logger
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan}


object Citybike_demo extends App {

  val logger = Logger("Demo")

  PrintTimings.set()
  PrintFlatPlan.set()
  PrintPhysicalPlan.set()

  val query = s"""call dbms.components() yield name, versions, edition unwind versions as version return name, version, edition;"""

  val query_2 =
    s"""|With duration({minutes : 5}) as _5m,
        |duration({minutes : 20}) as _20m
        |MATCH (s:Station)<-[r1:rentedAt]-(b1:Bike),
        |(b1)-[n1:returnedAt]->(p_Station),
        |(p)<-[r2:rentedAt]-(b2:Bike),
        |(b2)-[n2:returnedAt]->(o:Station)
        |WHERE r1.user_id = n1.user_id AND
        |n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND
        |n1.val_time < r2.val_time AND
        |duration.between(n1.val_time,r2.val_time) < _5m AND
        |duration.between(r1.val_time,n1.val_time) < _20m AND
        |duration.between(r2.val_time, n2.val_time) < _20m
        |RETURN r1.user_id, s.id, p.id, o.id""".stripMargin

  logger.info(s"Executing query: $query")


  implicit val memCypher: MemCypherSession = MemCypherSession.create

  val graph = MemCypherGraph.create(DemoData_citybike.nodes, DemoData_citybike.rels)

  graph.cypher(query).show

  Console.println("Hello World: ")

}

object DemoData_citybike {

  def nodes = Seq(n1, n2, n3, n4, n5, n6, n7, n8)

  def rels = Seq(e0, e1, e2, e3, e4, e5, e6, e7)


  val n1 = MemNode(1L, Set("Station"), CypherMap(
  ))

  val n2 = MemNode(2L, Set("Station"), CypherMap(
  ))

  val n3 = MemNode(3L, Set("Station"), CypherMap(
  ))

  val n4 = MemNode(4L, Set("Station"), CypherMap(
  ))

  val n5 = MemNode(5L, Set("Bike"), CypherMap(
  ))

  val n6 = MemNode(6L, Set("Bike"), CypherMap(
  ))

  val n8 = MemNode(8L, Set("Bike"), CypherMap(
  ))

  val n7 = MemNode(7L, Set("Bike"), CypherMap(
  ))


  val e0 = MemRelationship(0L, n5.id, n1.id, "rentedAt", CypherMap("user_id" -> 1234, "val_time" -> 52800000))
  val e1 = MemRelationship(1L, n6.id, n2.id, "rentedAt", CypherMap("user_id" -> 1234, "val_time" -> 53820000))
  val e2 = MemRelationship(2L, n8.id, n2.id, "rentedAt", CypherMap("user_id" -> 5678, "val_time" -> 53880000))
  val e3 = MemRelationship(3L, n7.id, n3.id, "rentedAt", CypherMap("user_id" -> 5678, "val_time" -> 55080000))
  val e4 = MemRelationship(4L, n5.id, n2.id, "returnedAt", CypherMap("user_id" -> 1234, "val_time" -> 53700000))
  val e5 = MemRelationship(5L, n6.id, n3.id, "returnedAt", CypherMap("user_id" -> 1234, "val_time" -> 54780000))
  val e6 = MemRelationship(6L, n8.id, n3.id, "returnedAt", CypherMap("user_id" -> 5678, "val_time" -> 54900000))
  val e7 = MemRelationship(7L, n7.id, n4.id, "returnedAt", CypherMap("user_id" -> 5678, "val_time" -> 56100000))



}

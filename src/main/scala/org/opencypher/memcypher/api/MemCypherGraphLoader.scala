package org.opencypher.memcypher.api

import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats, JValue}

import scala.io.Source

class MemCypherGraphLoader extends App {

  // Read the JSON file and parse it into a MemCypherGraph
  object Main extends App {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val filename = "demo.json"

    // Read the file content
    val fileContent = Source.fromFile(filename).mkString

    // Parse the JSON content
    val parsedJson: JValue = JsonMethods.parse(fileContent)

    // Convert the parsed JSON into a MemCypherGraph
    val result: Option[MemCypherGraph] = parsedJson.extractOpt[MemCypherGraph]

    // Handle the result
    result match {
      case Some(memCypherGraph) => println(s"Parsed MemCypherGraph: $memCypherGraph")
      case None => println("Error parsing JSON into MemCypherGraph")
    }
  }
}

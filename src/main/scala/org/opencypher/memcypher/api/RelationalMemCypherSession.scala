package org.opencypher.memcypher.api

import org.opencypher.okapi.relational.api.graph.RelationalCypherSession

class RelationalMemCypherSession extends RelationalCypherSession{
  override type Graph = this.type
  override type Records = this.type

  override private[opencypher] def records = ???

  override private[opencypher] def graphs = ???

  override private[opencypher] def entityTables = ???
}

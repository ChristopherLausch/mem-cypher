package org.opencypher.memcypher.api.configuration

import org.opencypher.okapi.impl.configuration.ConfigFlag

object MemCypherCoraConfiguration{

  object PrintFlatPlan extends ConfigFlag("cora.explainFlat")

  object PrintPhysicalPlan extends ConfigFlag("cora.explainPhysical", false)


}

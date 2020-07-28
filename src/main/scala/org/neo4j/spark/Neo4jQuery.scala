package org.neo4j.spark

import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import collection.JavaConverters._

object Neo4jQuery {

  import QueryType._

  val NODE_ALIAS = "n"
  val INTERNAL_ID_FIELD = "<id>"
  val INTERNAL_LABELS_FIELD = "<labels>"

  private val renderer: Renderer = Renderer.getDefaultRenderer

  def build(queryOptions: Neo4jQueryOptions): String =
    queryOptions.queryType match {
      case LABELS =>
        val labels: List[String] = queryOptions.value.split(":").toList
        val primaryLabel: String = labels.head
        val otherLabels: List[String] = labels.takeRight(labels.size - 1)
        val node = Cypher.node(primaryLabel, otherLabels.asJava).named(NODE_ALIAS)
        renderer.render(Cypher.`match`(node).returning(node).build())
      case _ => throw new NotImplementedError(s"'${queryOptions.queryType.toString.toLowerCase}' has not been implemented yet'")
    }
}
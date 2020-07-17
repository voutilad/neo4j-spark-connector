package org.neo4j.spark

import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import collection.JavaConverters._

object Neo4jQuery {
  import QueryType._

  private val renderer: Renderer = Renderer.getDefaultRenderer

  def build(queryOptions: Neo4jQueryOptions): String =
    queryOptions.queryType match {
      case NODE => {
        val labels: List[String] = queryOptions.value.split(":").toList
        val primaryLabel: String = labels.head
        val otherLabels: List[String] = labels.takeRight(labels.size - 1)
        val node = Cypher.node(primaryLabel, otherLabels.asJava).named("n")
        renderer.render(Cypher.`match`(node).returning(node).build())
      }
      case _ => throw new NotImplementedError(s"'${queryOptions.queryType.toString.toLowerCase}' has not been implemented yet'")
    }
}
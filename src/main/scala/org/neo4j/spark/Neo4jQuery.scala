package org.neo4j.spark.v2

import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.StatementBuilder.OngoingReadingAndReturn
import org.neo4j.cypherdsl.core.renderer.Renderer

class Neo4jQuery {
  import QueryType._

  private val cypherRenderer = Renderer.getDefaultRenderer

  def build(queryOptions: QueryOption, filters: Array[Filter]): String = if (queryType == TYPE_QUERY) value else {
    val statement: OngoingReadingAndReturn = queryOptions.queryType match {
      case TYPE_NODE =>
        val node = Cypher.node(queryOptions.value).named("t")
        val matchQuery = Cypher.`match`(node)

        matchQuery.where(
          filters.map {
            case filter: EqualTo => node.property(filter.attribute).isEqualTo(Cypher.literalOf(filter.value))
          }.reduce { (a, b) => a.and(b) }
        )

        matchQuery.returning(node)
      case TYPE_RELATIONSHIP =>
        val node1 = Cypher.anyNode("n1")
        val node2 = Cypher.anyNode("n2")
        node1.relationshipBetween(node2, value)
        val matchQuery = Cypher.`match`(node1)

        matchQuery.returning(node1, node2)

      case _ => throw new IllegalArgumentException("Something went wrong")
    }

    cypherRenderer.render(statement.build())
  }
}

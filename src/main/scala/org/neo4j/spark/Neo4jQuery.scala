package org.neo4j.spark

object Neo4jQuery {
  import QueryType._

  def build(queryOptions: QueryOption): String =
    if (queryOptions.queryType == QUERY) queryOptions.value
    else throw new NotImplementedError(s"'${queryOptions.queryType.toString.toLowerCase}' has not been implemented yet'")
}
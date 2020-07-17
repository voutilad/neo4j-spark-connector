package org.neo4j.spark

import org.junit.Test
import org.junit.Assert._

class Neo4jQueryTest {

  @Test
  def testNodeOneLabel(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.NODE.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = Neo4jQuery.build(neo4jOptions.query)

    assertEquals("MATCH (n:`Person`) RETURN n", query)
  }

  @Test
  def testNodeMultipleLabels(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.NODE.toString.toLowerCase, "Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = Neo4jQuery.build(neo4jOptions.query)

    assertEquals("MATCH (n:`Person`:`Player`:`Midfield`) RETURN n", query)
  }
}

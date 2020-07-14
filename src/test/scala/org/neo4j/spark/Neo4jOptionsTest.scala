package org.neo4j.spark

import java.util

import org.junit.jupiter.api.Assertions._
import org.junit.Test
import org.neo4j.driver.AccessMode
import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptionsTest {

  import org.junit.Rule
  import org.junit.rules.ExpectedException

  val _expectedException: ExpectedException = ExpectedException.none

  @Rule
  def exceptionRule: ExpectedException = _expectedException

  @Test
  def testUrlIsRequired(): Unit = {
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("Parameter 'url' is required")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryAndNodeShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH n RETURN n")
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'node', 'query', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryAndRelationshipShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH n RETURN n")
    options.put(QueryType.RELATIONSHIP.toString.toLowerCase, "KNOWS")

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'node', 'query', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testNodeAndRelationshipShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.NODE.toString.toLowerCase, "PERSON")
    options.put(QueryType.RELATIONSHIP.toString.toLowerCase, "KNOWS")

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'node', 'query', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryShouldHaveQueryType(): Unit = {
    val query: String = "MATCH n RETURN n"
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, query)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.QUERY, neo4jOptions.query.queryType)
    assertEquals(query, neo4jOptions.query.value)
  }

  @Test
  def testNodeShouldHaveNodeType(): Unit = {
    val label: String = "Person"
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.NODE.toString.toLowerCase, label)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.NODE, neo4jOptions.query.queryType)
    assertEquals(label, neo4jOptions.query.value)
  }

  @Test
  def testRelationshipShouldHaveRelationshipType(): Unit = {
    val relationship: String = "KNOWS"
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.NODE.toString.toLowerCase, relationship)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.NODE, neo4jOptions.query.queryType)
    assertEquals(relationship, neo4jOptions.query.value)
  }

  @Test
  def testDrierDefaults(): Unit = {
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH n RETURN n")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals("", neo4jOptions.session.database)
    assertEquals(AccessMode.READ, neo4jOptions.session.accessMode)
    assertEquals("basic", neo4jOptions.connection.auth)
    assertEquals("", neo4jOptions.connection.username)
    assertEquals("", neo4jOptions.connection.password)
    assertEquals(false, neo4jOptions.connection.encryption)
    assertEquals(TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, neo4jOptions.connection.trustStrategy)
    assertEquals("", neo4jOptions.connection.certificatePath)
    assertEquals(1000, neo4jOptions.connection.lifetime)
    assertEquals(1000, neo4jOptions.connection.timeout)
  }
}

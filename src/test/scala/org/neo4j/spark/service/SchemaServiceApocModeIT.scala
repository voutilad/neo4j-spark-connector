package org.neo4j.spark.service

import java.util
import java.util.UUID

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.{Before, Test}
import org.junit.Assert._
import org.neo4j.driver.Transaction
import org.neo4j.spark._

class SchemaServiceApocModeIT extends SparkConnectorScalaBaseApocTSE {

  @Before
  def beforeEach(): Unit = {
    SparkConnectorScalaSuiteApocIT.session()
      .writeTransaction((tx: Transaction) => tx.run("MATCH (n) DETACH DELETE n").consume())
  }

  @Test
  def testGetSchemaFromNodeBoolean(): Unit = {
    initTest("CREATE (p:Person {is_hero: true})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("is_hero", DataTypes.BooleanType))), schema)
  }

  @Test
  def testGetSchemaFromNodeString(): Unit = {
    initTest("CREATE (p:Person {name: 'John'})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("name", DataTypes.StringType))), schema)
  }

  @Test
  def testGetSchemaFromNodeLong(): Unit = {
    initTest("CREATE (p:Person {age: 93})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("age", DataTypes.IntegerType))), schema)
  }

  @Test
  def testGetSchemaFromNodeDouble(): Unit = {
    initTest("CREATE (p:Person {ratio: 43.120})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("ratio", DataTypes.DoubleType))), schema)
  }

  @Test
  def testGetSchemaFromNodePoint2D(): Unit = {
    initTest("CREATE (p:Person {location: point({x: 12.32, y: 49.32})})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("location", SchemaService.pointType))), schema)
  }

  @Test
  def testGetSchemaFromDate(): Unit = {
    initTest("CREATE (p:Person {born_on: date('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("born_on", DataTypes.DateType))), schema)
  }

  @Test
  def testGetSchemaFromDateTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: datetime('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("arrived_at", DataTypes.TimestampType))), schema)
  }

  @Test
  def testGetSchemaFromTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: time('125035.556+0100')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("arrived_at", DataTypes.TimestampType))), schema)
  }

  @Test
  def testGetSchemaFromStringArray(): Unit = {
    initTest("CREATE (p:Person {names: ['John', 'Doe']})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.StringType)))), schema)
  }

  @Test
  def testGetSchemaFromDateArray(): Unit = {
    initTest("CREATE (p:Person {names: [date('2019-11-19'), date('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.DateType)))), schema)
  }

  @Test
  def testGetSchemaFromTimestampArray(): Unit = {
    initTest("CREATE (p:Person {dates: [datetime('2019-11-19'), datetime('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("dates", DataTypes.createArrayType(DataTypes.TimestampType)))), schema)
  }

  @Test
  def testGetSchemaFromTimeArray(): Unit = {
    initTest("CREATE (p:Person {dates: [time('125035.556+0100'), time('125125.556+0100')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("dates", DataTypes.createArrayType(DataTypes.TimestampType)))), schema)
  }

  @Test
  def testGetSchemaFromIntegerArray(): Unit = {
    initTest("CREATE (p:Person {ages: [42, 101]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.NODE.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(StructType(Seq(StructField("ages", DataTypes.createArrayType(DataTypes.IntegerType)))), schema)
  }

  private def initTest(query: String): Unit = {
    SparkConnectorScalaSuiteApocIT.session()
      .writeTransaction((tx: Transaction) => tx.run(query).consume())
  }

  private def getSchema(options: java.util.Map[String, String]): StructType = {
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteApocIT.server.getBoltUrl)
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val schemaService: SchemaService = new SchemaService(neo4jOptions)

    val schema: StructType = schemaService.fromQuery()
    schemaService.close()

    new DriverCache(neo4jOptions.connection).close(neo4jOptions.uuid)

    schema
  }
}
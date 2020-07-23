package org.neo4j.spark.service

import java.util
import java.util.UUID

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.{Before, Test}
import org.junit.Assert._
import org.neo4j.driver.Transaction
import org.neo4j.spark._

class SchemaServiceNoApocModeIT extends SparkConnectorScalaBaseNoApocTSE {

  @Before
  def beforeEach(): Unit = {
    SparkConnectorScalaSuiteNoApocIT.session()
      .writeTransaction((tx: Transaction) => tx.run("MATCH (n) DETACH DELETE n").consume())
  }

  @Test
  def testGetSchemaFromNodeBoolean(): Unit = {
    initTest("CREATE (p:Person {is_hero: true})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("is_hero", DataTypes.BooleanType))), schema)
  }

  @Test
  def testGetSchemaFromNodeString(): Unit = {
    initTest("CREATE (p:Person {name: 'John'})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("name", DataTypes.StringType))), schema)
  }

  @Test
  def testGetSchemaFromNodeLong(): Unit = {
    initTest("CREATE (p:Person {age: 93})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("age", DataTypes.IntegerType))), schema)
  }

  @Test
  def testGetSchemaFromNodeDouble(): Unit = {
    initTest("CREATE (p:Person {ratio: 43.120})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("ratio", DataTypes.DoubleType))), schema)
  }

  @Test
  def testGetSchemaFromNodePoint2D(): Unit = {
    initTest("CREATE (p:Person {location: point({x: 12.32, y: 49.32})})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("location", SchemaService.pointType))), schema)
  }

  @Test
  def testGetSchemaFromDate(): Unit = {
    initTest("CREATE (p:Person {born_on: date('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("born_on", DataTypes.DateType))), schema)
  }

  @Test
  def testGetSchemaFromDateTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: datetime('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("arrived_at", SchemaService.timeType))), schema)
  }

  @Test
  def testGetSchemaFromTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: time('125035.556+0100')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("arrived_at", SchemaService.timeType))), schema)
  }

  @Test
  def testGetSchemaFromStringArray(): Unit = {
    initTest("CREATE (p:Person {names: ['John', 'Doe']})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.StringType)))), schema)
  }

  @Test
  def testGetSchemaFromDateArray(): Unit = {
    initTest("CREATE (p:Person {names: [date('2019-11-19'), date('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.DateType)))), schema)
  }

  @Test
  def testGetSchemaFromTimestampArray(): Unit = {
    initTest("CREATE (p:Person {dates: [datetime('2019-11-19'), datetime('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(SchemaService.timeType)))), schema)
  }

  @Test
  def testGetSchemaFromTimeArray(): Unit = {
    initTest("CREATE (p:Person {dates: [time('125035.556+0100'), time('125125.556+0100')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(SchemaService.timeType)))), schema)
  }

  @Test
  def testGetSchemaFromIntegerArray(): Unit = {
    initTest("CREATE (p:Person {ages: [42, 101]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("ages", DataTypes.createArrayType(DataTypes.IntegerType)))), schema)
  }

  @Test
  def testGetSchemaFromMultipleNodes(): Unit = {
    initTest(
      """
      CREATE (p1:Person {age: 31, name: 'Jane Doe'}),
        (p2:Person {name: 'John Doe', age: 33, location: null}),
        (p3:Person {age: 25, location: point({latitude: 12.12, longitude: 31.13})})
    """)

    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(
      StructField("age", DataTypes.IntegerType),
      StructField("location", SchemaService.pointType),
      StructField("name", DataTypes.StringType)
    )), schema)
  }

  private def getExpectedStructType(structFields: Seq[StructField]): StructType = {
    val additionalFields: Seq[StructField] = Seq(
      StructField(Neo4jQuery.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true),
      StructField(Neo4jQuery.INTERNAL_ID_FIELD, DataTypes.IntegerType, nullable = false)
    )
    StructType(structFields.union(additionalFields).reverse)
  }

  private def initTest(query: String): Unit = {
    SparkConnectorScalaSuiteNoApocIT.session()
      .writeTransaction((tx: Transaction) => tx.run(query).consume())
  }

  private def getSchema(options: java.util.Map[String, String]): StructType = {
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteNoApocIT.server.getBoltUrl)
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    val uuid: String = UUID.randomUUID().toString

    val schemaService: SchemaService = new SchemaService(neo4jOptions, uuid)

    val schema: StructType = schemaService.fromQuery()
    schemaService.close()

    new DriverCache(neo4jOptions.connection, uuid).close()

    schema
  }
}
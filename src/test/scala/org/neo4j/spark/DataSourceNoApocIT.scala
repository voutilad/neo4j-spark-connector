package org.neo4j.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.junit.Test
import org.neo4j.driver.Transaction
import org.junit.Assert._

class DataSourceNoApocIT extends SparkConnectorScalaBaseTSE {

  @Test
  def testReadNodeWithString(): Unit = {
    val name: String = "John"
    val df: DataFrame = initTest(s"CREATE (p:Person {name: '$name'})")

    assertEquals(name, df.select("name").collectAsList().get(0).getString(0))
  }

  @Test
  def testReadNodeWithInteger(): Unit = {
    val age: Integer = 42
    val df: DataFrame = initTest(s"CREATE (p:Person {age: $age})")

    assertEquals(age, df.select("age").collectAsList().get(0).getInt(0))
  }

  @Test
  def testReadNodeWithDouble(): Unit = {
    val score: Double = 3.14
    val df: DataFrame = initTest(s"CREATE (p:Person {score: $score})")

    assertEquals(score, df.select("score").collectAsList().get(0).getDouble(0), 0)
  }

  @Test
  def testReadNodeWithPoint(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({x: 12.12, y: 13.13})})")

    val res = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0);

    assertEquals(7203, res.get(0))
    assertEquals(12.12, res.get(1))
    assertEquals(13.13, res.get(2))
  }

  @Test
  def testReadNodeWithPoint3D(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({x: 12.12, y: 13.13, z: 1})})")

    val res = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0)

    assertEquals(9157, res.get(0))
    assertEquals(12.12, res.get(1))
    assertEquals(13.13, res.get(2))
    assertEquals(1.0, res.get(3))
  }

  @Test
  def testReadNodeWithDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {born: date('2009-10-10')})")

    df.select("born").show()
    val list = df.select("born").collectAsList()
    val res = list.get(0).getDate(0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res)
  }

  @Test
  def testReadNodeWithStringArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {names: ['John', 'Doe']})")

    val res = df.select("names").collectAsList().get(0).getAs[Seq[String]](0)

    assertEquals("John", res.head)
    assertEquals("Doe", res(1))
  }

  @Test
  def testReadNodeWithIntegerArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {ages: [22, 23]})")

    val res = df.select("ages").collectAsList().get(0).getAs[Seq[Integer]](0)

    assertEquals(22, res.head)
    assertEquals(23, res(1))
  }

  @Test
  def testReadNodeWithDoubleArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {scores: [22.33, 44.55]})")

    val res = df.select("scores").collectAsList().get(0).getAs[Seq[Double]](0)

    assertEquals(22.33, res.head, 0)
    assertEquals(44.55, res(1), 0)
  }

  @Test
  def testReadNodeWithBooleanArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {bools: [true, false]})")

    val res = df.select("bools").collectAsList().get(0).getAs[Seq[Boolean]](0)

    assertEquals(true, res.head)
    assertEquals(false, res(1))
  }

  @Test
  def testReadNodeWithPointArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {locations: [point({x: 11, y: 33.111}), point({x: 22, y: 44.222})]})")

    val res = df.select("locations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals(7203, res.head.get(0))
    assertEquals(11.0, res.head.get(1))
    assertEquals(33.111, res.head.get(2))

    assertEquals(7203, res(1).get(0))
    assertEquals(22.0, res(1).get(1))
    assertEquals(44.222, res(1).get(2))
  }

  @Test
  def testReadNodeWithPoint3DArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {locations: [point({x: 11, y: 33.111, z: 12}), point({x: 22, y: 44.222, z: 99.1})]})")

    val res = df.select("locations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals(9157, res.head.get(0))
    assertEquals(11.0, res.head.get(1))
    assertEquals(33.111, res.head.get(2))
    assertEquals(12.0, res.head.get(3))

    assertEquals(9157, res(1).get(0))
    assertEquals(22.0, res(1).get(1))
    assertEquals(44.222, res(1).get(2))
  }

  @Test
  def testReadNodeWithArrayDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {dates: [date('2009-10-10'), date('2009-10-11')]})")

    val res = df.select("dates").collectAsList().get(0).getAs[Seq[java.sql.Date]](0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res.head)
    assertEquals(java.sql.Date.valueOf("2009-10-11"), res(1))
  }

  @Test
  def testReadNodeRepartition(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
        |UNWIND people as p1
        |UNWIND range(1,10) as friend
        |WITH p1, people[(p1.id + friend) % size(people)] as p2
        |CREATE (p1)-[:KNOWS]->(p2)
        |RETURN *
    """.stripMargin

    val df: DataFrame = initTest(fixtureQuery)
    val repartitionedDf = df.repartition(10)

    assertEquals(10, repartitionedDf.rdd.getNumPartitions)
    val numNode = repartitionedDf.collect().length
    assertEquals(100, numNode)
  }

  private def initTest(query: String): DataFrame = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction((tx: Transaction) => tx.run(query).consume())

    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("node", "Person")
      .load()
  }
}

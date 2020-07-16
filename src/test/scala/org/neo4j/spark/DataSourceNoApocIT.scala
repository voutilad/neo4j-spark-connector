package org.neo4j.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes
import org.junit.{After, Ignore, Test}
import org.neo4j.driver.Transaction
import org.junit.Assert._

class DataSourceNoApocIT extends SparkConnectorScalaBaseTSE {

  @After
  def closeAll() = {
    DriverCache.closeAll()
  }

  @Test
  def testReadNodeWithString(): Unit = {
    val name: String = "John"
    val df: DataFrame = initTest(s"CREATE (p:Person {name: '$name'})")

    assertEquals(name, df.select("name").collectAsList().get(0).get(0))
  }

  @Test
  def testReadNodeWithInteger(): Unit = {
    val age: Integer = 42
    val df: DataFrame = initTest(s"CREATE (p:Person {age: $age})")

    assertEquals(age, df.select("age").collectAsList().get(0).get(0))
  }

  @Test
  def testReadNodeWithDouble(): Unit = {
    val score: Double = 3.14
    val df: DataFrame = initTest(s"CREATE (p:Person {score: $score})")

    assertEquals(score, df.select("score").collectAsList().get(0).get(0))
  }

  @Test
  def testReadNodeWithPoint(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({x: 12.12, y: 13.13})})")

    val res = df.select("location").collectAsList().get(0).get(0);

    assertEquals("[7203,12.12,13.13,NaN]", res.toString)
  }

  @Test
  @Ignore("Failing for some reason")
  def testReadNodeWithStringArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {names: ['John', 'Doe']})")

    val res = df.select("names").collectAsList().get(0).get(0);

    assertEquals("John", res.asInstanceOf[ArrayData].get(0, DataTypes.StringType))
    assertEquals("Doe", res.asInstanceOf[ArrayData].get(1, DataTypes.StringType))
  }

  @Test
  @Ignore("Failing for some reason")
  def testReadNodeWithIntegerArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {ages: [22, 23]})")

    val res = df.select("ages").collectAsList().get(0).get(0);

    assertEquals(22, res.asInstanceOf[ArrayData].getInt(0))
    assertEquals(23, res.asInstanceOf[ArrayData].getInt(1))
  }

  @Test
  @Ignore("Failing for some reason")
  def testReadNodeWithDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {born: date('2009-10-10')})")

    val res = df.select("born").collectAsList().get(0).get(0) // getDate(0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res)
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

package org.neo4j.spark

import org.apache.spark.sql.DataFrame
import org.junit.Assert.assertEquals
import org.junit.{Assume, Before, BeforeClass, Test}
import org.neo4j.driver.{SessionConfig, Transaction, TransactionWork}
import org.neo4j.driver.summary.ResultSummary

object DataSourceReaderNeo4j4xTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeFalse(TestUtil.neo4jVersion().startsWith("3.5"))
  }
}

class DataSourceReaderNeo4j4xTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testMultiDbJoin(): Unit = {
    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Customer {name: 'John Doe'}),
       (p2:Person:Customer {name: 'Mark Brown'}),
       (p3:Person:Customer {name: 'Cindy White'})
      """).consume()
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Employee {name: 'Jane Doe'}),
       (p2:Person:Employee {name: 'John Doe'})
      """).consume()
        })

    val df1 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("labels", "Person")
      .load()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("labels", "Person")
      .load()

    assertEquals(3, df1.count())
    assertEquals(2, df2.count())

    val dfJoin = df1.join(df2, df1("name") === df2("name"))
    assertEquals(1, dfJoin.count())
  }

  @Test
  def testReadQueryCustomPartitions(): Unit = {
    val fixtureProduct1Query: String =
      """CREATE (pr:Product{id: 1, name: 'Product 1'})
        |WITH pr
        |UNWIND range(1,100) as id
        |CREATE (p:Person {id: id, name: 'Person ' + id})-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureProduct1Query).consume()
        })
    val fixtureProduct2Query: String =
      """CREATE (pr:Product{id: 2, name: 'Product 2'})
        |WITH pr
        |UNWIND range(1,50) as id
        |MATCH (p:Person {id: id})
        |CREATE (p)-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureProduct2Query).consume()
        })

    val partitionedDf = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """
          |MATCH (p:Person)-[r:BOUGHT]->(pr:Product)
          |RETURN p.name AS person, pr.name AS product, r.quantity AS quantity""".stripMargin)
      .option("partitions", "5")
      .load()

    assertEquals(5, partitionedDf.rdd.getNumPartitions)
    val rows = partitionedDf.collect()
      .map(row => s"${row.getAs[String]("person")}-${row.getAs[String]("product")}")
    assertEquals(150, rows.size)
    assertEquals(150, rows.toSet.size)
  }

  @Test
  def testCallShouldReturnCorrectSchema(): Unit = {
    val callDf: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "CALL db.info() YIELD id, name RETURN *")
      .load()

    val res = callDf.select("name")
      .collectAsList()
      .get(0)

    assertEquals(res.getString(0), "neo4j")
  }

}

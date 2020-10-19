package org.neo4j.spark

import org.junit.{Assume, BeforeClass, Ignore, Test}

object DataSourceReaderNeo4j35TSE {
    @BeforeClass
    def checkNeo4jVersion() {
      Assume.assumeTrue(TestUtil.neo4jVersion().startsWith("3.5"))
    }
}

class DataSourceReaderNeo4j35TSE extends SparkConnectorScalaBaseTSE {
  @Test
  def testShouldThrowClearErrorIfADbIsSpecified(): Unit = {
    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("labels", "MATCH (h:Household) RETURN id(h)")
      .load()
      .show()
  }
}

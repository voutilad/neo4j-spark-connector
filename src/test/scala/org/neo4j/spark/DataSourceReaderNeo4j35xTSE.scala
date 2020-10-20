package org.neo4j.spark

import org.apache.spark.SparkException
import org.junit.Assert.{assertTrue, fail}
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.exceptions.ClientException

object DataSourceReaderNeo4j35xTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeTrue(TestUtil.neo4jVersion().startsWith("3.5"))
  }
}

class DataSourceReaderNeo4j35xTSE extends SparkConnectorScalaBaseTSE {
  @Test
  def testShouldThrowClearErrorIfADbIsSpecified(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("labels", "MATCH (h:Household) RETURN id(h)")
        .load()
        .show()
    }
    catch {
      case clientException: ClientException => {
        assertTrue(clientException.getMessage.equals(
          "Database name parameter for selecting database is not supported in Bolt Protocol Version 3.0. Database name: 'db1'"
        ))
      }
      case generic => fail(s"should be thrown a ${classOf[SparkException].getName}, got ${generic.getClass} instead")
    }

    @Test
    def testShouldThrowClearErrorIfACanComputeTheSchema(): Unit = {
      try {
        ss.read.format(classOf[DataSource].getName)
          .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
          .option("query", "MATCH (i:DO_NOT_EXIST) RETURN i")
          .load()
          .show()
      }
      catch {
        case clientException: ClientException => {
          assertTrue(clientException.getMessage.equals(
            "Unable to compute the resulting schema; this may mean your result set is empty or your version of Neo4j does not permit schema inference for empty sets"
          ))
        }
        case generic => fail(s"should be thrown a ${classOf[SparkException].getName}, got ${generic.getClass} instead")
      }
    }
  }
}

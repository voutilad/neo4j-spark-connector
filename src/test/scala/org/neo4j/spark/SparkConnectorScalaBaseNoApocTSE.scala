package org.neo4j.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

object SparkConnectorScalaBaseNoApocTSE {

  private var startedFromSuite = true

  @BeforeClass
  def setUpContainer() = {
    if (!SparkConnectorScalaSuiteNoApocIT.server.isRunning) {
      startedFromSuite = false
      SparkConnectorScalaSuiteNoApocIT.setUpContainer()
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (!startedFromSuite) {
      SparkConnectorScalaSuiteNoApocIT.tearDownContainer()
    }
  }

}

class SparkConnectorScalaBaseNoApocTSE {

  val conf: SparkConf = SparkConnectorScalaSuiteNoApocIT.conf
  val ss: SparkSession = SparkConnectorScalaSuiteNoApocIT.ss

  val _testName: TestName = new TestName

  @Rule
  def testName = _testName

  @Before
  def before() {
    SparkConnectorScalaSuiteNoApocIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
      })
  }

  @After
  def after() {
    if (!Option(System.getenv("TRAVIS")).getOrElse("false").toBoolean) {
      try {
        utils.Assert.assertEventually(new utils.Assert.ThrowingSupplier[Boolean, Exception] {
          override def get(): Boolean = {
            val afterConnections = SparkConnectorScalaSuiteNoApocIT.getActiveConnections
            SparkConnectorScalaSuiteNoApocIT.connections == afterConnections
          }
        }, Matchers.equalTo(true), 60, TimeUnit.SECONDS)
      } finally {
        val afterConnections = SparkConnectorScalaSuiteNoApocIT.getActiveConnections
        if (SparkConnectorScalaSuiteNoApocIT.connections != afterConnections) { // just for debug purposes
          println(s"For test ${testName.getMethodName} => connections before: ${SparkConnectorScalaSuiteNoApocIT.connections}, after: $afterConnections")
        }
      }
    }
  }

}

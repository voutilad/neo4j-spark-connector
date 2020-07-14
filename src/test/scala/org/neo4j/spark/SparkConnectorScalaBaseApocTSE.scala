package org.neo4j.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.hamcrest.Matchers
import org.junit._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach}
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

object SparkConnectorScalaBaseApocTSE {

  private var startedFromSuite = true

  @BeforeClass
  def setUpContainer() = {
    if (!SparkConnectorScalaSuiteApocIT.server.isRunning) {
      startedFromSuite = false
      SparkConnectorScalaSuiteApocIT.setUpContainer()
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (!startedFromSuite) {
      SparkConnectorScalaSuiteApocIT.tearDownContainer()
    }
  }

}

class SparkConnectorScalaBaseApocTSE {

  val conf: SparkConf = SparkConnectorScalaSuiteApocIT.conf
  val sc: SparkContext = SparkConnectorScalaSuiteApocIT.sc

  val _testName: TestName = new TestName

  @Rule
  def testName = _testName

  @BeforeEach
  def before() {
    SparkConnectorScalaSuiteApocIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
      })
  }

  @AfterEach
  def after() {
    if (!Option(System.getenv("TRAVIS")).getOrElse("false").toBoolean) {
      try {
        utils.Assert.assertEventually(new utils.Assert.ThrowingSupplier[Boolean, Exception] {
          override def get(): Boolean = {
            val afterConnections = SparkConnectorScalaSuiteApocIT.getActiveConnections
            SparkConnectorScalaSuiteApocIT.connections == afterConnections
          }
        }, Matchers.equalTo(true), 60, TimeUnit.SECONDS)
      } finally {
        val afterConnections = SparkConnectorScalaSuiteApocIT.getActiveConnections
        if (SparkConnectorScalaSuiteApocIT.connections != afterConnections) { // just for debug purposes
          println(s"For test ${testName.getMethodName} => connections before: ${SparkConnectorScalaSuiteApocIT.connections}, after: $afterConnections")
        }
      }
    }
  }

}

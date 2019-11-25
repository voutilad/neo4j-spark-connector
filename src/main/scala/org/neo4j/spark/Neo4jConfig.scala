package org.neo4j.spark

import org.apache.spark.SparkConf
import org.neo4j.driver.v1.{Driver, AuthTokens, Config, GraphDatabase}

/**
 * @author mh
 * @since 02.03.16
 */
case class Neo4jConfig(url: String, user: String = "neo4j", password: Option[String] = None, encryptionStatus: Boolean) {
  //If the encryptionStatus variable is false, use the withEncryption() method to initialize the config instance,
  // otherwise use the withoutEncryption() to initialize it.
  private def createBoltConfig() = if (encryptionStatus) Config.build().withEncryption().toConfig else Config.build().withoutEncryption().toConfig

  def driver(config: Neo4jConfig): Driver = config.password match {
    case Some(pwd) => GraphDatabase.driver(config.url, AuthTokens.basic(config.user, pwd), createBoltConfig())
    case _ => GraphDatabase.driver(config.url, createBoltConfig())
  }

  def driver(): Driver = driver(this)

  def driver(url: String): Driver = GraphDatabase.driver(url, createBoltConfig())

}

object Neo4jConfig {
  // List of currently supported parameters
  private val URL = "spark.neo4j.bolt.url"
  private val USER = "spark.neo4j.bolt.user"
  private val PASSWORD = "spark.neo4j.bolt.password"
  private val ENCRYPTION_STATUS = "spark.neo4j.bolt.encryption.status"

  def apply(sparkConf: SparkConf): Neo4jConfig = {
    val url = sparkConf.get(URL, "bolt://localhost")
    val user = sparkConf.get(USER, "neo4j")
    val password: Option[String] = sparkConf.getOption(PASSWORD)
    // default value is false
    val encryptionStatus = sparkConf.getBoolean(ENCRYPTION_STATUS, defaultValue = false)
    Neo4jConfig(url, user, password, encryptionStatus)
  }
}

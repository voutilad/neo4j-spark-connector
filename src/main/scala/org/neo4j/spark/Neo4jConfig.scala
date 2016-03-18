package org.neo4j.spark

import org.apache.spark.SparkConf
import org.neo4j.driver.v1.{GraphDatabase, Config}

/**
  * @author mh
  * @since 02.03.16
  */
object Neo4jConfig {
  def apply(sparkConf: SparkConf) : BoltConfig = {
    val url = sparkConf.get("neo4j.bolt.url", "bolt://localhost")
    // todo
    // val user = sparkConf.get("neo4j.bolt.user", "neo4j")
    // val password: Option[String] = sparkConf.getOption("neo4j.bolt.password")
    BoltConfig(url)
  }

  def boltConfig() = Config.build.withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig

  def driver(url: String) = GraphDatabase.driver( url, boltConfig() )
}
case class BoltConfig(val url:String)

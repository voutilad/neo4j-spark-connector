package org.neo4j.spark

import org.apache.spark.SparkConf

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
}
case class BoltConfig(val url:String)

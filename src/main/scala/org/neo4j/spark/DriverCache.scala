package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.driver.{Driver, GraphDatabase}
import org.neo4j.spark.DriverCache.{cache, idCache}
import org.neo4j.spark.util.Neo4jUtil

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val idCache = new ConcurrentHashMap[String, Boolean]
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Driver = {
    idCache.put(jobId, true)
    cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => GraphDatabase.driver(t.url))
  }

  def close(): Unit = {
    idCache.remove(jobId)
    if(idCache.isEmpty) {
      val driver = cache.remove(options)
      if (driver != null) {
        Neo4jUtil.closeSafety(driver)
      }
    }
  }
}
package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.driver.{Driver, GraphDatabase}
import org.neo4j.spark.DriverCache.{cache, jobIdCache}
import org.neo4j.spark.util.Neo4jUtil

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val jobIdCache = new ConcurrentHashMap[String, Boolean]
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Driver = {
    jobIdCache.put(jobId, true)
    cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => GraphDatabase.driver(t.url, t.toNeo4jAuth, t.toDriverConfig))
  }

  def close(): Unit = {
    jobIdCache.remove(jobId)
    if(jobIdCache.isEmpty) {
      val driver = cache.remove(options)
      if (driver != null) {
        Neo4jUtil.closeSafety(driver)
      }
    }
  }
}
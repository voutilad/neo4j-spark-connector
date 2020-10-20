package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, function}

import org.neo4j.driver.{Driver, GraphDatabase, Session, SessionConfig}
import org.neo4j.spark.DriverCache.{cache, jobIdCache}
import org.neo4j.spark.util.Neo4jUtil

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Driver = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, new function.Function[Neo4jDriverOptions, Driver] {
        override def apply(t: Neo4jDriverOptions): Driver = GraphDatabase.driver(t.url, t.toNeo4jAuth, t.toDriverConfig)
      })
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if(jobIdCache.isEmpty) {
        val driver = cache.remove(options)
        if (driver != null) {
          Neo4jUtil.closeSafety(driver)
        }
      }
    }
  }
}

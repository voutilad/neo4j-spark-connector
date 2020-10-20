package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, function}

import org.neo4j.driver.{Driver, GraphDatabase, Session, SessionConfig}
import org.neo4j.spark.DriverCache.{cache, jobIdCache}
import org.neo4j.spark.util.Neo4jUtil

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Neo4jDriverWrapper] = new ConcurrentHashMap[Neo4jDriverOptions, Neo4jDriverWrapper]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Neo4jDriverWrapper = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, new function.Function[Neo4jDriverOptions, Neo4jDriverWrapper] {
        override def apply(t: Neo4jDriverOptions): Neo4jDriverWrapper = new Neo4jDriverWrapper(GraphDatabase.driver(t.url, t.toNeo4jAuth, t.toDriverConfig))
      })
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if(jobIdCache.isEmpty) {
        val driverWrapper = cache.remove(options)
        if (driverWrapper != null) {
          Neo4jUtil.closeSafety(driverWrapper.getDriver)
        }
      }
    }
  }
}

class Neo4jDriverWrapper(driver: Driver) {
  def session(sessionConfig: SessionConfig): Session = {
    val session = driver.session(sessionConfig)
    session.run("EXPLAIN MATCH (n) RETURN count(n)").consume()
    session
  }

  def getDriver: Driver = driver
}
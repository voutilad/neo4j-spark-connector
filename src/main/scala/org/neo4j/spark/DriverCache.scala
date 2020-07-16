package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap
import org.neo4j.driver.{Driver, GraphDatabase}
import org.neo4j.spark.DriverCache.{cache, idCache}

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val idCache = new ConcurrentHashMap[String, Boolean]

  def closeAll(): Unit = {
    cache.values().forEach(d => d.close())
    cache.clear()
  }
}

class DriverCache(private val options: Neo4jDriverOptions) extends Serializable {
  def getOrCreate(uuid: String): Driver = {
    idCache.put(uuid, true)
    cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => GraphDatabase.driver(t.url))
  }

  def close(uuid: String): Unit = {
    idCache.remove(uuid)
    if(idCache.isEmpty) {
      val driver = cache.remove(options)
      if (driver != null) {
        driver.close()
      }
    }
  }
}
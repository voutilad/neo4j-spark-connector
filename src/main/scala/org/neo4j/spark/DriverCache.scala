package org.neo4j.spark

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.driver.{Driver, GraphDatabase}

object DriverCache {

  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]

  def getOrCreate(options: Neo4jDriverOptions): Driver = cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => GraphDatabase.driver(t.url))

  def delete(options: Neo4jDriverOptions): Unit = cache.remove(options)
}

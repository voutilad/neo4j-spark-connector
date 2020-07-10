package org.neo4j.spark.v2.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.{Result, Session}
import org.neo4j.spark.v2.Neo4jOptions

class Neo4jInputPartitionReader(pushedFilters: Array[Filter], options: Neo4jOptions) extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  var customIterator: Result = _
  var session: Session = _

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(pushedFilters, options)

  def next: Boolean = {
    if (customIterator == null) {
      session = DriverCache.getOrCreate(options.connection).session()

      val query = options.query.build(pushedFilters)
      println(s"Executing $query")
      customIterator = session
        .run(query)
    }

    customIterator.hasNext
  }

  def get: InternalRow = {
    val name = UTF8String.fromString(customIterator.next().get("name").asString());
    println("Getting a row " + name)
    InternalRow(name)
  }

  def close(): Unit = session.close()

}
package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.{Result, Session}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery}

class Neo4jInputPartitionReader(options: Neo4jOptions) extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  var customIterator: Result = _
  var session: Session = _

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options)

  def next: Boolean = {
    if (customIterator == null) {
      session = DriverCache.getOrCreate(options.connection).session()
      customIterator = session.run(Neo4jQuery.build(options.queryOption))
    }

    customIterator.hasNext
  }

  def get: InternalRow = {
    // todo check
    val name = UTF8String.fromString(customIterator.next().get("name").asString());
    InternalRow(name)
  }

  def close(): Unit = session.close()

}
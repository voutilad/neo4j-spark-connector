package org.neo4j.spark.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Record, Session, Transaction}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery, QueryType}
import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jInputPartitionReader(private val options: Neo4jOptions,
                                private val schema: StructType,
                                private val jobId: String) extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  var result: Iterator[Record] = _
  var session: Session = _
  var transaction: Transaction = _
  var driverCache: DriverCache = new DriverCache(options.connection, jobId)

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options, schema, jobId)

  def next: Boolean = {
    if (result == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
      transaction = session.beginTransaction()
      result = transaction.run(Neo4jQuery.build(options.query)).list.asScala.iterator
    }

    result.hasNext
  }

  private def getRecordMap(record: Record): java.util.Map[String, Object] = {
    options.query.queryType match {
      case QueryType.LABELS =>
        val node = record.get(Neo4jQuery.NODE_ALIAS).asNode()
        val nodeMap: java.util.Map[String, Object] = new util.HashMap[String, Object](node.asMap())
        nodeMap.put(Neo4jQuery.INTERNAL_ID_FIELD, node.id().asInstanceOf[Object])
        nodeMap.put(Neo4jQuery.INTERNAL_LABELS_FIELD, node.labels().asInstanceOf[Object])
        nodeMap
      case _ => throw new IllegalArgumentException(s"Query type `${options.query.queryType}` not supported")
    }
  }

  def get: InternalRow = {
    val record = getRecordMap(result.next())
    InternalRow.fromSeq(schema.map(field => {
      Neo4jUtil.convertFromNeo4j(record.get(field.name))
    }))
  }

  def close(): Unit = {
    Neo4jUtil.closeSafety(transaction)
    Neo4jUtil.closeSafety(session)
    driverCache.close()
  }

}
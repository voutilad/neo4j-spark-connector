package org.neo4j.spark.reader

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Record, Session, Transaction}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery, QueryType}
import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jInputPartitionReader(private val options: Neo4jOptions,
                                private val schema: StructType,
                                private val jobId: String) extends InputPartition[InternalRow]
  with InputPartitionReader[InternalRow]
  with Logging {

  private var result: Iterator[Record] = _
  private var session: Session = _
  private var transaction: Transaction = _
  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options, schema, jobId)

  def next: Boolean = {
    if (result == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
      transaction = session.beginTransaction()
      val query = Neo4jQuery.build(options.query)
      log.info(s"Running the following query on Neo4j: $query")
      result = transaction.run(query).list.asScala.iterator
    }

    result.hasNext
  }

  private def getRecordMap(record: Record): java.util.Map[String, Any] = {
    options.query.queryType match {
      case QueryType.LABELS =>
        val node = record.get(Neo4jQuery.NODE_ALIAS).asNode()
        val nodeMap = new util.HashMap[String, Any](node.asMap())
        nodeMap.put(Neo4jQuery.INTERNAL_ID_FIELD, node.id())
        nodeMap.put(Neo4jQuery.INTERNAL_LABELS_FIELD, node.labels())
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
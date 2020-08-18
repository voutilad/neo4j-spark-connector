package org.neo4j.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Record, Session, Transaction}
import org.neo4j.spark.service.{MappingService, Neo4jQueryReadStrategy, Neo4jQueryService, Neo4jReadMappingStrategy}
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{DriverCache, Neo4jOptions}

import scala.collection.JavaConverters._

class Neo4jInputPartitionReader(private val options: Neo4jOptions,
                                private val filters: Array[Filter],
                                private val schema: StructType,
                                private val jobId: String) extends InputPartition[InternalRow]
  with InputPartitionReader[InternalRow]
  with Logging {

  private var result: Iterator[Record] = _
  private var session: Session = _
  private var transaction: Transaction = _
  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private val query: String = new Neo4jQueryService(options, new Neo4jQueryReadStrategy(filters)).createQuery()

  private val mappingService = new MappingService(new Neo4jReadMappingStrategy(options), options)

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options, filters, schema, jobId)

  def next: Boolean = {
    if (result == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
      transaction = session.beginTransaction()
      log.info(s"Running the following query on Neo4j: $query")
      result = transaction.run(query).asScala
    }

    result.hasNext
  }

  def get: InternalRow = mappingService.convert(result.next(), schema)

  def close(): Unit = {
    Neo4jUtil.closeSafety(transaction)
    Neo4jUtil.closeSafety(session)
    driverCache.close()
  }

}
package org.neo4j.spark.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.{DriverCache, Neo4jOptions}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Validations

import scala.collection.JavaConverters._

class Neo4jDataSourceReader(private val options: DataSourceOptions, private val jobId: String) extends DataSourceReader
  with SupportsPushDownFilters {

  private var filters: Array[Filter] = Array[Filter]()

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))


  override def readSchema(): StructType = callSchemaService { schemaService => schemaService
    .struct() }

  private def callSchemaService[T](lambda: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    var hasError = false
    try {
      lambda(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  private def getPartitions(): Seq[(Long, Long)] = callSchemaService { schemaService => schemaService
    .skipLimitFromPartition() }

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    val schema = readSchema()
    val partitionSkipLimit = getPartitions()
    val neo4jPartitions = if (partitionSkipLimit.isEmpty) {
      Seq(new Neo4jInputPartitionReader(neo4jOptions, filters, schema, jobId))
    } else {
      partitionSkipLimit.zipWithIndex
        .map(triple => new Neo4jInputPartitionReader(neo4jOptions, filters, schema, jobId, triple._2,
          triple._1._1, triple._1._2))
    }
    new java.util.ArrayList[InputPartition[InternalRow]](neo4jPartitions.asJava)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters
}
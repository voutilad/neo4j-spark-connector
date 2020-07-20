package org.neo4j.spark.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.Neo4jOptions
import org.neo4j.spark.service.SchemaService

class Neo4jDataSourceReader(private val options: DataSourceOptions, private val jobId: String) extends DataSourceReader {

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())

  override def readSchema(): StructType = new SchemaService(neo4jOptions, jobId).fromQuery()

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    val schema  = readSchema()
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new Neo4jInputPartitionReader(neo4jOptions, schema, jobId))
    factoryList
  }
}
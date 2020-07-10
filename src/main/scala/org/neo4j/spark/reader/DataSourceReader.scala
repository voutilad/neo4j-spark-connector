package org.neo4j.spark.v2.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.v2.Neo4jOptions

class DataSourceReader(options: DataSourceOptions) extends DataSourceReader with SupportsPushDownFilters {

  val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())

  override def readSchema(): StructType = StructType(Array())

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new NeoInputPartitionReader(pushedFilters, neo4jOptions))
    factoryList
  }

  var pushedFilters: Array[Filter] = Array[Filter]()

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilters = filters
    pushedFilters
  }
}
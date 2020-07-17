package org.neo4j.spark

import java.util.UUID

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.neo4j.spark.reader.Neo4jDataSourceReader

class DataSource extends DataSourceV2 with ReadSupport with DataSourceRegister {

  val jobId: String = UUID.randomUUID().toString

  def createReader(options: DataSourceOptions) = new Neo4jDataSourceReader(options, jobId)

  override def shortName(): String = "neo4j"
}
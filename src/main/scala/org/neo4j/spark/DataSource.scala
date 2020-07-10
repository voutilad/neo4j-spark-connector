package org.neo4j.spark

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DataSource extends DataSourceV2 with ReadSupport with DataSourceRegister {
  def createReader(options: DataSourceOptions) = new NeoDataSourceReader(options)

  override def shortName(): String = "neo4j"
}
package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.util.Validations
import org.neo4j.spark.{DriverCache, Neo4jOptions, NodeWriteMode}

class Neo4jDataSourceWriter(jobId: String,
                            structType: StructType,
                            saveMode: SaveMode,
                            options: DataSourceOptions) extends DataSourceWriter {

  private val optionsMap = options.asMap()
  optionsMap.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(optionsMap)
    .validate(neo4jOptions => Validations.writer(neo4jOptions, jobId, saveMode))

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new Neo4jDataWriterFactory(jobId, structType, saveMode, neo4jOptions)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }
}
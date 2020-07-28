package org.neo4j.spark.util

import org.apache.spark.sql.SaveMode
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.{DriverCache, Neo4jOptions, QueryType}

object Validations {

  val writer: (Neo4jOptions, String, SaveMode) => Unit = { (neo4jOptions, jobId, saveMode) =>
    if (neo4jOptions.session.accessMode == AccessMode.READ) {
      throw new IllegalArgumentException(s"Mode READ not supported for Data Source writer")
    }
    val schemaService = new SchemaService(neo4jOptions, jobId)
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    try {
      neo4jOptions.query.queryType match {
        case QueryType.QUERY => {
          if (schemaService.isReadQuery(s"WITH {} AS event ${neo4jOptions.query.value}")) {
            throw new IllegalArgumentException(s"Please provide a valid WRITE query")
          }
        }
        case QueryType.LABELS => {
          saveMode match {
            case SaveMode.Overwrite => {
              if (neo4jOptions.nodeMetadata.nodeKeys.isEmpty) {
                throw new IllegalArgumentException(s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite")
              }
            }
            case _ => Unit
          }
        }
      }
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}

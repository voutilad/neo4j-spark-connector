package org.neo4j.spark.util

import org.apache.spark.sql.SaveMode
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.{DriverCache, Neo4jOptions, QueryType}

object Validations {

  val writer: (Neo4jOptions, String, SaveMode) => Unit = { (neo4jOptions, jobId, saveMode) =>
    ValidationUtil.isFalse(neo4jOptions.session.accessMode == AccessMode.READ,
      s"Mode READ not supported for Data Source writer")
    val schemaService = new SchemaService(neo4jOptions, jobId)
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    try {
      neo4jOptions.query.queryType match {
        case QueryType.QUERY => {
          ValidationUtil.isFalse(schemaService.isReadQuery(s"WITH {} AS event ${neo4jOptions.query.value}"),
            "Please provide a valid WRITE query")
        }
        case QueryType.LABELS => {
          saveMode match {
            case SaveMode.Overwrite => {
              ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.nodeKeys,
                s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite")
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

  val read: (Neo4jOptions, String) => Unit = { (neo4jOptions, jobId) =>
    val schemaService = new SchemaService(neo4jOptions, jobId)
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    try {
      neo4jOptions.query.queryType match {
        case QueryType.LABELS => {
          ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.labels,
            s"You need to set the ${QueryType.LABELS.toString.toLowerCase} option")
        }
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotBlank(neo4jOptions.relationshipMetadata.relationshipType,
            s"You need to set the ${QueryType.RELATIONSHIP.toString.toLowerCase} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.source.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} option")
        }
        case QueryType.QUERY => {
          ValidationUtil.isTrue(schemaService.isReadQuery(neo4jOptions.query.value),
            "Please provide a valid READ query")
        }
      }
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}

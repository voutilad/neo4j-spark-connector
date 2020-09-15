package org.neo4j.spark.util

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.{DriverCache, Neo4jOptions, QueryType}
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit

import scala.collection.mutable

object Validations {

  val writer: (Neo4jOptions, String, SaveMode) => Unit = { (neo4jOptions, jobId, saveMode) =>
    ValidationUtil.isFalse(neo4jOptions.session.accessMode == AccessMode.READ,
      s"Mode READ not supported for Data Source writer")
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
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
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} is required when Save Mode is Overwrite")
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} is required when Save Mode is Overwrite")
          schemaService.structForRelationship()
        }
      }
    } finally {
      schemaService.close()
      cache.close()
    }
  }

  val schemaOptions: (Neo4jOptions, StructType) => Unit = { (neo4jOptions, schema) =>
    val missingFieldsMap: mutable.Map[String, Set[String]] = mutable.HashMap.empty

    missingFieldsMap.put(
      Neo4jOptions.NODE_KEYS,
      schema.missingFields(neo4jOptions.nodeMetadata.nodeKeys.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.NODE_PROPS,
      schema.missingFields(neo4jOptions.nodeMetadata.nodeProps.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.RELATIONSHIP_PROPERTIES,
      schema.missingFields(neo4jOptions.relationshipMetadata.properties.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_PROPS,
      schema.missingFields(neo4jOptions.relationshipMetadata.source.nodeProps.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_KEYS,
      schema.missingFields(neo4jOptions.relationshipMetadata.source.nodeKeys.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_PROPS,
      schema.missingFields(neo4jOptions.relationshipMetadata.target.nodeProps.keySet)
    )
    missingFieldsMap.put(
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_KEYS,
      schema.missingFields(neo4jOptions.relationshipMetadata.target.nodeKeys.keySet)
    )

    val optionsWithMissingFields = missingFieldsMap.filter(_._2.nonEmpty)

    if (optionsWithMissingFields.nonEmpty) {
      throw new IllegalArgumentException("Write failed due to the following errors:\n" +
        optionsWithMissingFields.map(field => s" - Schema is missing ${field._2.mkString(", ")} from option `${field._1}`").mkString("\n") +
        "\n\nThe option key and value might be inverted.")
    }
  }

  val read: (Neo4jOptions, String) => Unit = { (neo4jOptions, jobId) =>
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
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
          if (neo4jOptions.queryMetadata.queryCount.nonEmpty) {
            if (!Neo4jUtil.isLong(neo4jOptions.queryMetadata.queryCount)) {
              ValidationUtil.isTrue(schemaService.isValidQueryCount(neo4jOptions.queryMetadata.queryCount),
                "Please provide a valid READ query count")
            }
          }
        }
      }
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}

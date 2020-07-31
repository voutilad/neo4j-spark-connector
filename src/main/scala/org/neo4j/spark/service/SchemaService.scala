package org.neo4j.spark.service

import java.util.Collections

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.{Record, Session}
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.spark.QueryType
import org.neo4j.spark.service.SchemaService.{durationType, pointType, timeType}
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{DriverCache, Neo4jOptions}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SchemaService(private val options: Neo4jOptions, private val jobId: String) extends AutoCloseable {

  private val cypherRenderer = Renderer.getDefaultRenderer

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private val session: Session = driverCache.getOrCreate().session(options.session.toNeo4jSession)

  private def cypherToSparkType(cypherType: String): DataType = {
    cypherType match {
      case "Boolean" => DataTypes.BooleanType
      case "String" => DataTypes.StringType
      case "Long" => DataTypes.LongType
      case "Double" => DataTypes.DoubleType
      case "Point" | "InternalPoint2D" | "InternalPoint3D" => pointType
      case "LocalDateTime"  | "DateTime" | "ZonedDateTime" => DataTypes.TimestampType
      case "OffsetTime" | "Time" | "LocalTime" => timeType
      case "LocalDate" | "Date" => DataTypes.DateType
      case "Duration" | "InternalIsoDuration" => durationType
      case "StringArray" => DataTypes.createArrayType(DataTypes.StringType)
      case "LongArray" => DataTypes.createArrayType(DataTypes.LongType)
      case "DoubleArray" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "BooleanArray" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "PointArray" | "InternalPoint2DArray" | "InternalPoint3DArray" => DataTypes.createArrayType(pointType)
      case "LocalDateTimeArray" | "DateTimeArray" | "ZonedDateTimeArray" => DataTypes.createArrayType(DataTypes.TimestampType)
      case "OffsetTimeArray" | "TimeArray" | "LocalTimeArray" => DataTypes.createArrayType(timeType)
      case "LocalDateArray" | "DateArray" => DataTypes.createArrayType(DataTypes.DateType)
      case "DurationArray" | "InternalIsoDurationArray" => DataTypes.createArrayType(durationType)
      case _ => DataTypes.StringType
    }
  }

  private def structForNode(labels: Seq[String] = options.nodeMetadata.labels): StructType = {
    var structFields: mutable.Buffer[StructField] = (try {
        val query = "CALL apoc.meta.nodeTypeProperties({ includeLabels: $labels })"
        val params = Map[String, AnyRef]("labels" -> labels.asJava)
          .asJava
        retrieveSchemaFromApoc(query, params)
      } catch {
        case e: ClientException =>
          e.code match {
            case "Neo.ClientError.Procedure.ProcedureNotFound" => {
              // TODO get back to Cypher DSL when rand function will be available
              val query = s"""MATCH (${Neo4jUtil.NODE_ALIAS}:${labels.mkString(":")})
                |RETURN ${Neo4jUtil.NODE_ALIAS}
                |ORDER BY rand()
                |LIMIT ${options.query.schemaFlattenLimit}
                |""".stripMargin
              val params = Collections.emptyMap[String, AnyRef]()
              retrieveSchema(query, params, { record => record.get(Neo4jUtil.NODE_ALIAS).asNode.asMap.asScala.toMap })
            }
          }
      })
      .sortBy(t => t.name)

    structFields += StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true)
    structFields += StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false)
    StructType(structFields.reverse)
  }

  private def retrieveSchemaFromApoc(query: String, params: java.util.Map[String, AnyRef]): mutable.Buffer[StructField] = {
    session.run(query, params)
      .list
      .asScala
      .filter(record => !record.get("propertyName").isNull && !record.get("propertyName").isEmpty)
      .map(record => StructField(record.get("propertyName").asString,
        cypherToSparkType(record.get("propertyTypes").asList.get(0).toString)))
  }

  private def retrieveSchema(query: String,
                             params: java.util.Map[String, AnyRef],
                             extractFunction: Record => Map[String, AnyRef]): mutable.Buffer[StructField] = {
    session.run(query, params).list.asScala
      .flatMap(extractFunction)
      .groupBy(_._1)
      .map(t => {
        val value = t._2.head._2
        StructField(t._1, cypherToSparkType(value match {
          case l: java.util.List[_] => s"${l.get(0).getClass.getSimpleName}Array"
          case _ => value.getClass.getSimpleName
        }))
      })
      .toBuffer
  }

  private def mapStructField(alias: String, field: StructField): StructField = {
    val name = field.name match {
      case Neo4jUtil.INTERNAL_ID_FIELD | Neo4jUtil.INTERNAL_LABELS_FIELD =>
        s"<$alias.${field.name.replaceAll("[<|>]", "")}>"
      case _ => s"$alias.${field.name}"
    }
    StructField(name, field.dataType, field.nullable, field.metadata)
  }

  def structForRelationship(): StructType = {
    var structFields: mutable.Buffer[StructField] = ArrayBuffer(
      StructField(Neo4jUtil.INTERNAL_ID_FIELD.replace("id", "rel.id"), DataTypes.LongType, false),
      StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, false))

    if (options.relationshipMetadata.nodeMap) {
      structFields += StructField(s"<${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}>",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
      structFields += StructField(s"<${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}>",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
    } else {
      structFields ++= structForNode(options.relationshipMetadata.source.labels)
        .map(field => mapStructField(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, field))
      structFields ++= structForNode(options.relationshipMetadata.target.labels)
        .map(field => mapStructField(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, field))
    }

    structFields ++= (try {
        val query = "CALL apoc.meta.relTypeProperties({ includeRels: $rels })"
        val params = Map[String, AnyRef]("rels" -> Seq(options.relationshipMetadata.relationshipType).asJava)
          .asJava
        retrieveSchemaFromApoc(query, params)
      } catch {
        case e: ClientException =>
          e.code match {
            case "Neo.ClientError.Procedure.ProcedureNotFound" => {
              // TODO get back to Cypher DSL when rand function will be available
              val query = s"""MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}:${options.relationshipMetadata.source.labels.mkString(":")})
                |MATCH (${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}:${options.relationshipMetadata.target.labels.mkString(":")})
                |MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${options.relationshipMetadata.relationshipType}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
                |RETURN ${Neo4jUtil.RELATIONSHIP_ALIAS}
                |ORDER BY rand()
                |LIMIT ${options.query.schemaFlattenLimit}
                |""".stripMargin
              val params = Collections.emptyMap[String, AnyRef]()
              retrieveSchema(query, params, { record => record.get(Neo4jUtil.RELATIONSHIP_ALIAS).asRelationship.asMap.asScala.toMap })
            }
          }
      })
      .map(field => StructField(s"rel.${field.name}", field.dataType, field.nullable, field.metadata))
      .sortBy(t => t.name)
    StructType(structFields)
  }

  def structForQuery(): StructType = StructType(Array.empty[StructField])

  def struct(): StructType = {
    options.query.queryType match {
      case QueryType.LABELS => structForNode()
      case QueryType.RELATIONSHIP => structForRelationship()
      case QueryType.QUERY => structForQuery()
    }
  }

  def isReadQuery(query: String): Boolean = {
    val queryType = session.run(s"EXPLAIN $query").consume().queryType()
    queryType == org.neo4j.driver.summary.QueryType.READ_ONLY || queryType == org.neo4j.driver.summary.QueryType.SCHEMA_WRITE
  }

  override def close(): Unit = {
    Neo4jUtil.closeSafety(session)
  }
}

object SchemaService {
  val POINT_TYPE_2D = "point-2d"
  val POINT_TYPE_3D = "point-3d"

  val TIME_TYPE_OFFSET = "offset-time"
  val TIME_TYPE_LOCAL = "local-time"

  val DURATION_TYPE = "duration"

  val durationType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("months", DataTypes.LongType, false),
    DataTypes.createStructField("days", DataTypes.LongType, false),
    DataTypes.createStructField("seconds", DataTypes.LongType, false),
    DataTypes.createStructField("nanoseconds", DataTypes.IntegerType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))

  val pointType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("srid", DataTypes.IntegerType, false),
    DataTypes.createStructField("x", DataTypes.DoubleType, false),
    DataTypes.createStructField("y", DataTypes.DoubleType, false),
    DataTypes.createStructField("z", DataTypes.DoubleType, true)
  ))

  val timeType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))
}
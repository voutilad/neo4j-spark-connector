package org.neo4j.spark.service

import java.util.Collections

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.Session
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery}

import collection.JavaConverters._

class SchemaService(private val options: Neo4jOptions, private val jobId: String) extends AutoCloseable {

  import SchemaService._

  import org.neo4j.spark.QueryType._

  private val cypherRenderer = Renderer.getDefaultRenderer

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private val session: Session = driverCache.getOrCreate().session(options.session.toNeo4jSession)

  private def cypherToSparkType(cypherType: String): DataType = {
    cypherType match {
      case "Boolean" => DataTypes.BooleanType
      case "String" => DataTypes.StringType
      case "Long" => DataTypes.IntegerType
      case "Double" => DataTypes.DoubleType
      case "Point" | "InternalPoint2D" | "InternalPoint3D" => pointType
      case "LocalDateTime" | "DateTime" | "ZonedDateTime" | "OffsetTime" | "Time" => DataTypes.TimestampType
      case "LocalDate" | "Date" => DataTypes.DateType
      case "StringArray" | "DurationArray" | "InternalIsoDurationArray" => DataTypes.createArrayType(DataTypes.StringType)
      case "LongArray" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "DoubleArray" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "BooleanArray" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "PointArray" | "InternalPoint2DArray" | "InternalPoint3DArray" => DataTypes.createArrayType(pointType)
      case "LocalDateTimeArray" | "DateTimeArray" | "ZonedDateTimeArray" | "OffsetTimeArray"
           | "TimeArray" => DataTypes.createArrayType(DataTypes.TimestampType)
      case "LocalDateArray" | "DateArray" => DataTypes.createArrayType(DataTypes.DateType)
      case _ => DataTypes.StringType
    }
  }

  def queryForNode(): StructType = {
    val structFields = try {
      session.run(
        "CALL apoc.meta.nodeTypeProperties({ includeLabels: $labels })",
        Collections.singletonMap[String, Object]("labels", options.query.value.split(":").toSeq.asJava)
      ).list.asScala.map(record => {
        StructField(record.get("propertyName").asString, cypherToSparkType(record.get("propertyTypes").asList.get(0).toString))
      })
    } catch {
      case e: ClientException =>
        e.code match {
          case "Neo.ClientError.Procedure.ProcedureNotFound" =>
            val node = Cypher.node(options.query.value).named(Neo4jQuery.NODE_ALIAS)
            session.run(
              cypherRenderer.render(Cypher.`match`(node).returning(node).limit(1).build())
            ).list.asScala.flatMap(record => {
              record.get(Neo4jQuery.NODE_ALIAS).asNode.asMap.asScala.map(t => {
                StructField(t._1, cypherToSparkType(t._2 match {
                  case l: java.util.List[Any] => s"${l.get(0).getClass.getSimpleName}Array"
                  case _ => t._2.getClass.getSimpleName
                }))
              })
            })
        }
    }

    structFields += StructField(Neo4jQuery.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true)
    structFields += StructField(Neo4jQuery.INTERNAL_ID_FIELD, DataTypes.IntegerType, nullable = false)
    StructType(structFields.reverse)
  }

  def queryForRelationship(): StructType = StructType(Array.empty[StructField])

  def query(): StructType = StructType(Array.empty[StructField])

  def fromQuery(): StructType = {
    options.query.queryType match {
      case LABELS => queryForNode()
      case RELATIONSHIP => queryForRelationship()
      case QUERY => query()
    }
  }

  override def close(): Unit = {
    Neo4jUtil.closeSafety(session)
  }
}

object SchemaService {
  val pointType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("srid", DataTypes.IntegerType, false),
    DataTypes.createStructField("x", DataTypes.DoubleType, false),
    DataTypes.createStructField("y", DataTypes.DoubleType, false),
    DataTypes.createStructField("z", DataTypes.DoubleType, true),
  ))
}
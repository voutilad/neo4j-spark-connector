package org.neo4j.spark.service

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.{Record, Session}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery}

import collection.JavaConverters._

class SchemaService(val options: Neo4jOptions) extends AutoCloseable {

  import SchemaService._

  import org.neo4j.spark.QueryType._

  private val cypherRenderer = Renderer.getDefaultRenderer

  private val session: Session = DriverCache.getOrCreate(options.connection).session()

  private def mapType(internalType: String): DataType = {
    internalType match {
      case "Boolean" => DataTypes.BooleanType
      case "String" => DataTypes.StringType
      case "Long" => DataTypes.IntegerType
      case "Double" => DataTypes.DoubleType
      case "Point" | "InternalPoint2D" | "InternalPoint3D" => pointType
      case "LocalDateTime" | "DateTime" | "ZonedDateTime" | "OffsetTime" | "Time" => DataTypes.TimestampType
      case "LocalDate" | "Date" => DataTypes.DateType
      case "StringArray" => DataTypes.createArrayType(DataTypes.StringType)
      case "LongArray" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "DoubleArray" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "BooleanArray" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "PointArray" => DataTypes.createArrayType(pointType)
      case "LocalDateTimeArray" | "DateTimeArray" | "ZonedDateTimeArray" | "OffsetTimeArray" | "TimeArray" => DataTypes.createArrayType(DataTypes.TimestampType)
      case "LocalDateArray" | "DateArray" => DataTypes.createArrayType(DataTypes.DateType)
      // @todo handle Map type for QUERY
      case _ => DataTypes.StringType
    }
  }

  def queryForNode(): StructType = {
    val structFields = try {
      session.run(
        "CALL apoc.meta.nodeTypeProperties({ includeLabels: $labels })",
        Map[String, AnyRef]("labels" -> options.query.value.split(":").toSeq.asJava).asJava
      ).list.asScala.map(record => {
        StructField(record.get("propertyName").asString, mapType(record.get("propertyTypes").asList.get(0).toString))
      })
    } catch {
      case e: ClientException =>
        e.code match {
          case "Neo.ClientError.Procedure.ProcedureNotFound" =>
            val node = Cypher.node(options.query.value).named("n")
            session.run(
              cypherRenderer.render(Cypher.`match`(node).returning(node).limit(1).build())
            ).list.asScala.flatMap(record => {
              record.get("n").asNode.asMap.asScala.map(t => {
                StructField(t._1, mapType(t._2 match {
                  case l: java.util.List[Any] => s"${l.get(0).getClass.getSimpleName}Array"
                  case _ => t._2.getClass.getSimpleName
                }))
              })
            })
        }
    }

    StructType(structFields)
  }

  def queryForRelationship(): StructType = StructType(Array.empty[StructField])

  def query(): StructType = StructType(Array.empty[StructField])

  def fromQuery(): StructType = {
    options.query.queryType match {
      case NODE => queryForNode()
      case RELATIONSHIP => queryForRelationship()
      case QUERY => query()
    }
  }

  override def close(): Unit = session.close()
}

object SchemaService {
  val pointType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("srid", DataTypes.StringType, false),
    DataTypes.createStructField("latitude", DataTypes.DoubleType, true),
    DataTypes.createStructField("longitude", DataTypes.DoubleType, true),
    DataTypes.createStructField("x", DataTypes.DoubleType, true),
    DataTypes.createStructField("y", DataTypes.DoubleType, true),
    DataTypes.createStructField("z", DataTypes.DoubleType, true),
  ))
}
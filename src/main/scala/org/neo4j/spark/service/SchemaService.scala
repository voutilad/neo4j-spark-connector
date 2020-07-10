package org.neo4j.spark.service

import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.driver.types.Type
import org.neo4j.driver.{Record, Session}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery}

import collection.JavaConverters._

class SchemaService(val options: Neo4jOptions) {

  import org.neo4j.spark.QueryType._

  private val session: Session = DriverCache.getOrCreate(options.connection).session()

  def mapType(value: String): DataType = {

    value match {
      case "Boolean" => DataTypes.BooleanType
      case "String" => DataTypes.StringType
      case "Long" => DataTypes.IntegerType
      case "Double" => DataTypes.DoubleType
      /*case "Point" => // @todo con mappa [String, String]
      case "Date" => // @todo
      case "Map" => // @todo
      case "List" => // @todo*/
      case _ => DataTypes.StringType
    }
  }

  def queryForNode(): StructType = {
    val structFields = try {
      session.run(
        "CALL apoc.meta.nodeTypeProperties({ includeLabels: $labels })",
        Map[String, AnyRef]("labels" -> options.queryOption.value.split(":").toSeq.asJava).asJava
      ).list.asScala.map( record => {
        StructField(record.get("propertyName").asString, mapType(record.get("propertyTypes").asList.get(0).toString))
      })
    } catch {
      case _ /* @todo check status Neo.ClientError.Procedure.ProcedureNotFound  */ => session.run(
        s"MATCH (n:{${options.queryOption.value}) RETURN n LIMIT 1"
      ).list.asScala.flatMap( record => {
        record.get("n").asNode.asMap.asScala.map(t => {
          StructField(t._1, mapType(t._2.getClass.getSimpleName))
        })
      })
    }

    StructType(structFields)
  }

  def queryForRelationship(): StructType = StructType(Array.empty[StructField])

  def query(): StructType = StructType(Array.empty[StructField])

  def fromQuery(): StructType = {
    options.queryOption.queryType match {
      case NODE => queryForNode()
      case RELATIONSHIP => queryForRelationship()
      case QUERY => query()
    }
  }
}

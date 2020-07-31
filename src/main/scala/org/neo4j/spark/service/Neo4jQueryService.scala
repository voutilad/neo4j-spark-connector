package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.{Neo4jOptions, QueryType}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jQueryWriteStrategy(private val saveMode: SaveMode) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String =
    s"""UNWIND ${"$"}events AS event
      |${options.query.value}
      |""".stripMargin

  override def createStatementForRelationships(options: Neo4jOptions): String = throw new UnsupportedOperationException("TODO implement method")

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val keyword = saveMode match {
      case SaveMode.Overwrite => "MERGE"
      case SaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
    val labels = options.nodeMetadata.labels
      .map(_.quote)
      .mkString(":")
    val keys = options.nodeMetadata.nodeKeys.keys
      .map(_.quote)
      .map(k => s"$k: event.keys.$k")
      .mkString(", ")
    s"""UNWIND ${"$"}events AS event
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += event.properties
       |""".stripMargin
  }
}

class Neo4jQueryReadStrategy extends Neo4jQueryStrategy {
  private val renderer: Renderer = Renderer.getDefaultRenderer

  override def createStatementForQuery(options: Neo4jOptions): String = throw new UnsupportedOperationException("TODO implement")

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    s"""MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}:${options.relationshipMetadata.source.labels.mkString(":")})
       |MATCH (${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}:${options.relationshipMetadata.target.labels.mkString(":")})
       |MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${options.relationshipMetadata.relationshipType}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
       |RETURN ${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}, ${Neo4jUtil.RELATIONSHIP_ALIAS}, ${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}
       |""".stripMargin
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val primaryLabel = options.nodeMetadata.labels.head
    val otherLabels = options.nodeMetadata.labels.takeRight(options.nodeMetadata.labels.size - 1)
    val node = Cypher.node(primaryLabel, otherLabels.asJava).named(Neo4jUtil.NODE_ALIAS)
    renderer.render(Cypher.`match`(node).returning(node).build())
  }
}

abstract class Neo4jQueryStrategy {
  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String
}

class Neo4jQueryService(private val options: Neo4jOptions, private val strategy: Neo4jQueryStrategy) extends Serializable {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY => strategy.createStatementForQuery(options)
    case _ => throw new UnsupportedOperationException(
      s"""Query Type not supported.
         |You provided ${options.query.queryType},
         |supported types: ${QueryType.values.mkString(",")}""".stripMargin)
  }
}

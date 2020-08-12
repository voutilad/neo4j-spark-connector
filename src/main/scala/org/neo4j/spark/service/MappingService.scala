package org.neo4j.spark.service

import java.util
import java.util.function
import java.util.function.BiConsumer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.{Record, Value, Values}
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{Neo4jOptions, QueryType, RelationshipWriteStrategy}

import scala.collection.JavaConverters._
import scala.collection.mutable

class Neo4jWriteMappingStrategy(private val options: Neo4jOptions)
  extends Neo4jMappingStrategy[InternalRow, java.util.Map[String, AnyRef]] {
  override def node(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val rowMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val keys: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val properties: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    rowMap.put("keys", keys)
    rowMap.put("properties", properties)

    query(row, schema)
      .forEach(new BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit = if (options.nodeMetadata.nodeKeys.contains(key)) {
          keys.put(key, value)
        } else {
          properties.put(key, value)
        }
      })

    rowMap
  }

  private def relationshipNativeMap(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val rowMap: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]

    val relMap = new util.HashMap[String, AnyRef]()
    val sourceMap = new util.HashMap[String, AnyRef]()
    val targetMap = new util.HashMap[String, AnyRef]()

    query(row, schema)
      .forEach(new BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit =
          if (key.startsWith(Neo4jUtil.RELATIONSHIP_ALIAS.concat("."))) {
            relMap.put(key.split('.').drop(1).mkString("."), value)
          }
          else if (key.startsWith(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS.concat("."))) {
            sourceMap.put(key.split('.').drop(1).mkString("."), value)
          }
          else if (key.startsWith(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS.concat("."))) {
            targetMap.put(key.split('.').drop(1).mkString("."), value)
          }
      })

    rowMap.put(Neo4jUtil.RELATIONSHIP_ALIAS, relMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, sourceMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, targetMap)

    rowMap
  }

  private def relationshipKeysMap(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val rowMap: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]

    val relMap = new util.HashMap[String, AnyRef]()
    val sourceMap = new util.HashMap[String, AnyRef]()
    val targetMap = new util.HashMap[String, AnyRef]()

    query(row, schema)
      .forEach(new BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit =
          if (options.relationshipMetadata.source.nodeKeys.contains(key)) {
            sourceMap.put(key, value)
          }
          else if (options.relationshipMetadata.target.nodeKeys.contains(key)) {
            targetMap.put(key, value)
          }
          else {
            relMap.put(key, value)
          }
      })

    rowMap.put(Neo4jUtil.RELATIONSHIP_ALIAS, relMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, sourceMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, targetMap)

    rowMap
  }

  override def relationship(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    options.relationshipMetadata.writeStrategy match {
      case RelationshipWriteStrategy.NATIVE => relationshipNativeMap(row, schema)
      case RelationshipWriteStrategy.KEYS => relationshipKeysMap(row, schema)
    }
  }

  override def query(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val seq = row.toSeq(schema)
    (0 to schema.size - 1)
      .flatMap(i => {
        val field = schema(i)
        val neo4jValue = Neo4jUtil.convertFromSpark(seq(i), field)
        neo4jValue match {
          case map: MapValue => Neo4jUtil.flattenMap(map.asMap(), field.name)
            .asScala
            .map(t => (t._1, Values.value(t._2)))
            .toSeq
          case _ => Seq((field.name, neo4jValue))
        }
      })
      .toMap
      .asJava
  }
}

class Neo4jReadMappingStrategy(private val options: Neo4jOptions) extends Neo4jMappingStrategy[Record, InternalRow] {

  override def node(record: Record, schema: StructType): InternalRow = {
    val node = record.get(Neo4jUtil.NODE_ALIAS).asNode()
    val nodeMap = new util.HashMap[String, Any](node.asMap())
    nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, node.id())
    nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, node.labels())

    mapToInternalRow(nodeMap, schema)
  }

  private def mapToInternalRow(map: util.Map[String, Any],
                               schema: StructType) = InternalRow.fromSeq(schema
    .map(field => Neo4jUtil.convertFromNeo4j(map.get(field.name), field.dataType)))

  private def flatRelNodeMapping(node: Node, alias: String): mutable.Map[String, Any] = {
    val nodeMap: mutable.Map[String, Any] = node.asMap().asScala
      .map(t => (s"$alias.${t._1}", t._2))
    nodeMap.put(s"<$alias.${
      Neo4jUtil.INTERNAL_ID_FIELD
        .replaceAll("[<|>]", "")
    }>",
      node.id())
    nodeMap.put(s"<$alias.${
      Neo4jUtil.INTERNAL_LABELS_FIELD
        .replaceAll("[<|>]", "")
    }>",
      node.labels())
    nodeMap
  }

  private def mapRelNodeMapping(node: Node, alias: String): Map[String, util.Map[String, String]] = {
    val nodeMap: util.Map[String, String] = new util.HashMap[String, String](node.asMap(new function.Function[Value, String] {
      override def apply(t: Value): String = t.toString
    }))

    nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, Neo4jUtil.mapper.writeValueAsString(node.id()))
    nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, Neo4jUtil.mapper.writeValueAsString(node.labels()))
    Map(s"<$alias>" -> nodeMap)
  }

  override def relationship(record: Record, schema: StructType): InternalRow = {
    val rel = record.get(Neo4jUtil.RELATIONSHIP_ALIAS).asRelationship()
    val relMap = new util.HashMap[String, Any](rel.asMap())
      .asScala
      .map(t => (s"rel.${t._1}", t._2))
      .asJava
    relMap.put(Neo4jUtil.INTERNAL_REL_ID_FIELD, rel.id())
    relMap.put(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, rel.`type`())

    val source = record.get(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS).asNode()
    val target = record.get(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS).asNode()

    val (sourceMap, targetMap) = if (options.relationshipMetadata.nodeMap) {
      (mapRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
        mapRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS))
    } else {
      (flatRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
        flatRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS))
    }

    relMap.putAll(sourceMap.toMap.asJava)
    relMap.putAll(targetMap.toMap.asJava)

    mapToInternalRow(relMap, schema)
  }

  override def query(elem: Record, schema: StructType): InternalRow = mapToInternalRow(elem.asMap(new function.Function[Value, Any] {
    override def apply(t: Value): Any = t.asObject()
  }), schema)
}

abstract class Neo4jMappingStrategy[IN, OUT] extends Serializable {
  def node(elem: IN, schema: StructType): OUT

  def relationship(elem: IN, schema: StructType): OUT

  def query(elem: IN, schema: StructType): OUT
}

class MappingService[IN, OUT](private val strategy: Neo4jMappingStrategy[IN, OUT], private val options: Neo4jOptions) extends Serializable {

  def convert(record: IN, schema: StructType): OUT = options.query.queryType match {
    case QueryType.LABELS => strategy.node(record, schema)
    case QueryType.RELATIONSHIP => strategy.relationship(record, schema)
    case QueryType.QUERY => strategy.query(record, schema)
  }

}
package org.neo4j.spark.service

import org.neo4j.driver.{Record, Result}
import org.neo4j.driver.types.{Node, Relationship, Type}

object FlatteningService {

  import java.util
  import org.neo4j.driver.internal.types.InternalTypeSystem

  private var keys: Array[String] = Array[String]()
  private var classes: Array[Type] = Array[Type]()

  private val ACCEPTED_TYPES_FOR_FLATTENING = util.Arrays.asList("NODE", "RELATIONSHIP")
//
//  def flattenResultSet(r: Result): Map[String, String] = {
//    r.forEachRemaining(flattenRecord)
//    (keys zip classes.map(_.name())).toMap
//  }
//
//  private def flattenRecord(r: Record): Unit = {
//    r.fields.forEach(pair => {
//      if (keys.indexOf(pair.key).eq(-1)) {
//        keys += (pair.key)
//        classes += (r.get(pair.key).`type`)
//      }
//      val value = r.get(pair.key)
//      if (ACCEPTED_TYPES_FOR_FLATTENING.get(0).equals(pair.value.`type`.name)) {
//        flattenNode(value.asNode, pair.key)
//      }
//      else if (ACCEPTED_TYPES_FOR_FLATTENING.get(1).equals(pair.value.`type`.name)) {
//        flattenRelationship(value.asRelationship, pair.key)
//      }
//    })
//  }
//
//  private def flattenNode(node: Node, nodeKey: String): Unit = {
//    if (keys.indexOf(nodeKey + ".id") eq -1) {
//      keys += (nodeKey + ".id")
//      classes += InternalTypeSystem.TYPE_SYSTEM.INTEGER
//      keys += (nodeKey + ".labels")
//      classes += InternalTypeSystem.TYPE_SYSTEM.LIST
//    }
//    node.keys.forEach(key => {
//      if (keys.indexOf(nodeKey + "." + key) eq -1) {
//        keys += (nodeKey + "." + key)
//        classes += (node.get(key).`type`)
//      }
//    })
//  }
//
//  private def flattenRelationship(rel: Relationship, relationshipKey: String): Unit = {
//    if (keys.indexOf(relationshipKey + ".id") eq -1) {
//      keys += (relationshipKey + ".id")
//      classes += (InternalTypeSystem.TYPE_SYSTEM.INTEGER)
//      keys += (relationshipKey + ".type")
//      classes += (InternalTypeSystem.TYPE_SYSTEM.STRING)
//    }
//    rel.keys.forEach(key => {
//      if (keys.indexOf(relationshipKey + "." + key) eq -1) {
//        keys += (relationshipKey + "." + key)
//        classes += (rel.get(key).`type`)
//      }
//    })
//  }
}

package org.neo4j.spark

import java.util
import java.util.{Collections, NoSuchElementException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.{GraphDatabase, Values}

import scala.collection.JavaConverters._

class CypherRDD(@transient sc: SparkContext, val query: String, val parameters: java.util.Map[String,Any])
  extends RDD[java.util.Map[String,AnyRef]](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[java.util.Map[String,AnyRef]] = {
    val session = GraphDatabase.driver(config.url).session()

    val result = session.run(query,parameters.asScala.mapValues( Values.value ).asJava)
    var keys : Array[String] = null
    var keyCount : Int = 0
    new Iterator[java.util.Map[String,AnyRef]]() {
      var hasNext: Boolean = result.next()

      override def next(): java.util.Map[String,AnyRef] = {
        if (hasNext) {
          val record = result.record()
          if (keys == null) {
            val keysList = result.keys()
            keyCount = keysList.size()
            keys = keysList.toArray(new Array[String](keyCount))
          }
          if (keyCount == 0) return Collections.emptyMap()
          if (keyCount == 1) return Collections.singletonMap(keys(0),record.get(0).asObject())
          val builder = new util.LinkedHashMap[String,AnyRef](keyCount)
          var i = 0
          while (i < keyCount) {
            builder.put(keys(i), record.get(i).asObject())
            i = i + 1
          }
          hasNext = result.next()
          builder
        } else throw new NoSuchElementException
      }
    }
}
  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherRDD {
  // java apply
  def apply(sc: SparkContext, query: String, parameters:java.util.Map[String,Any]) = new CypherRDD(sc, query, parameters)
  // scala apply
//  def apply(sc: SparkContext, query: String, parameters:Map[String,Any]) = new CypherRDD(sc, query, parameters )
}

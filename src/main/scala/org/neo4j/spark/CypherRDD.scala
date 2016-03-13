package org.neo4j.spark

import java.util
import java.util.{Collections, NoSuchElementException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.{Value, GraphDatabase, Values}

import scala.collection.JavaConverters._

class CypherRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String,Any)])
  extends RDD[util.Map[String,AnyRef]](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[util.Map[String,AnyRef]] = {
    val session = GraphDatabase.driver(config.url).session()

    val params: Map[String,Value] = parameters.map( (p) => (p._1, Values.value(p._2))).toMap
    val result = session.run(query,params.asJava)
    var keys : Array[String] = null
    var keyCount : Int = 0
    new Iterator[java.util.Map[String,AnyRef]]() {
      var hasNext: Boolean = result.next()

      override def next(): util.Map[String,AnyRef] = {
        if (hasNext) {
          val record = result.record()
          if (keys == null) {
            val keysList = result.keys()
            keyCount = keysList.size()
            keys = keysList.toArray(new Array[String](keyCount))
          }
          val res : util.Map[String,AnyRef] = if (keyCount == 0)  Collections.emptyMap()
          else if (keyCount == 1)  Collections.singletonMap(keys(0),record.get(0).asObject())
          else {
            val builder = new util.LinkedHashMap[String, AnyRef](keyCount)
            var i = 0
            while (i < keyCount) {
              builder.put(keys(i), record.get(i).asObject())
              i = i + 1
            }
            builder
          }
          hasNext = result.next()
          if (!hasNext) {
            session.close()
          }
          res
        } else throw new NoSuchElementException
      }
    }
}
  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherRDD {
  def apply(sc: SparkContext, query: String, parameters:util.Map[String,Any]) = new CypherRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq)
  def apply(sc: SparkContext, query: String, parameters:Seq[(String,Any)] = Seq.empty) = new CypherRDD(sc, query, parameters)
}

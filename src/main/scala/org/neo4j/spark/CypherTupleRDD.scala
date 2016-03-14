package org.neo4j.spark

import java.util
import java.util.{Collections, NoSuchElementException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.{Driver, Value, GraphDatabase, Values}

import scala.collection.JavaConverters._

class CypherTupleRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, Any)])
  extends RDD[Seq[(String, AnyRef)]](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[(String, AnyRef)]] = {
    val driver: Driver = GraphDatabase.driver(config.url)
    val session = driver.session()

    val params: Map[String, Value] = parameters.map((p) => (p._1, Values.value(p._2))).toMap
    val result = session.run(query, params.asJava)

    var keys: Array[String] = null
    var keyCount: Int = 0
    new Iterator[Seq[(String, AnyRef)]]() {
      def advance = {
        val r = result.next()
        if (!r) {
          session.close()
          driver.close()
        }
        r
      }
      var hasNext: Boolean = advance

      override def next(): Seq[(String, AnyRef)] = {
        if (hasNext) {
          val record = result.record()
          if (keys == null) {
            val keysList = result.keys()
            keyCount = keysList.size()
            keys = keysList.toArray(new Array[String](keyCount))
          }
          val res =
            if (keyCount == 0) Seq.empty
            else if (keyCount == 1) Seq(keys(0) -> record.get(0).asObject())
            else {
              val builder = Seq.newBuilder[(String, AnyRef)]
              var i = 0
              while (i < keyCount) {
                builder += keys(i) -> record.get(i).asObject()
                i = i + 1
              }
              builder.result()
            }
          hasNext = advance
          res
        } else throw new NoSuchElementException
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherTupleRDD {
  def apply(sc: SparkContext, query: String, parameters: java.util.Map[String, Any]) = new CypherTupleRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq)
  def apply(sc: SparkContext, query: String, parameters: Seq[(String,Any)] = Seq.empty) = new CypherTupleRDD(sc, query, parameters)
}



package org.neo4j.spark

import java.util
import java.util.{Collections, NoSuchElementException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.{Driver, Value, GraphDatabase, Values}

import scala.collection.JavaConverters._

class CypherTupleRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, AnyRef)])
  extends RDD[Seq[(String, AnyRef)]](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[(String, AnyRef)]] = {
    val driver: Driver = Neo4jConfig.driver(config.url)
    val session = driver.session()

    val result = session.run(query, parameters.toMap.asJava)

    result.asScala.map( (record) => {
      val res = record.asMap().asScala.toSeq
      if (!result.hasNext) {
        session.close()
        driver.close()
      }
       res
    })
  }

  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherTupleRDD {
  def apply(sc: SparkContext, query: String, parameters: java.util.Map[String, AnyRef]) = new CypherTupleRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq)
  def queryMap(sc: SparkContext, query: String, parameters: java.util.Map[String, AnyRef]) : RDD[java.util.Map[String,AnyRef]] = new CypherTupleRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq).map( (t) => new java.util.LinkedHashMap[String,AnyRef](t.toMap.asJava) )
  def apply(sc: SparkContext, query: String, parameters: Seq[(String,AnyRef)] = Seq.empty) = new CypherTupleRDD(sc, query, parameters)
}



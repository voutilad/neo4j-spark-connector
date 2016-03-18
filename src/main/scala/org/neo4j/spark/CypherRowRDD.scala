package org.neo4j.spark

import java.util.NoSuchElementException

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.neo4j.driver.v1._

import scala.collection.JavaConverters._

class CypherRowRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String,AnyRef)])
  extends RDD[Row](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val driver = Neo4jConfig.driver(config.url)
    val session = driver.session()

    val result : StatementResult = session.run(query,parameters.toMap.asJava)

    result.asScala.map( (record) => {
      val keyCount = record.size()

      val res = if (keyCount == 0)  Row.empty
      else if (keyCount == 1)  Row(record.get(0).asObject())
      else {
        val builder = Seq.newBuilder[AnyRef]
        var i = 0
        while (i < keyCount) {
          builder += record.get(i).asObject()
          i = i + 1
        }
        Row.fromSeq(builder.result())
      }
      if (!result.hasNext) {
        session.close()
        driver.close()
      }
      res
    })
}
  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherRowRDD {
  def apply(sc: SparkContext, query: String, parameters:java.util.Map[String,AnyRef]) = new CypherRowRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq)
  def apply(sc: SparkContext, query: String, parameters:Seq[(String,AnyRef)] = Seq.empty) = new CypherRowRDD(sc, query, parameters)
}

package org.neo4j.spark

import java.util.NoSuchElementException

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, types}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1._
import scala.collection.JavaConverters._

class CypherResultRdd(@transient sc: SparkContext, result : ResultCursor)
  extends RDD[Row](sc, Nil)
    with Logging {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val keyCount = result.size()

    new Iterator[Row]() {
      var hasNext: Boolean = true

      override def next(): Row = {
        if (hasNext) {
          val record = result.record()
          if (keyCount == 0) return Row.empty
          if (keyCount == 1) return Row(record.get(0).asObject())
          val builder = Seq.newBuilder[AnyRef]
          builder.sizeHint(keyCount)
          var i = 0
          while (i < keyCount) {
            builder += record.get(i).asObject()
            i = i + 1
          }
          hasNext = result.next()
          Row.fromSeq(builder.result())
        } else throw new NoSuchElementException
      }
    }
}
  override protected def getPartitions: Array[Partition] = Array(new DummyPartition())
}

object CypherDataFrame {
  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, Any], resultSchema: Array[(String, Type)]) = {

    val fields = resultSchema.toSeq.map( field =>
      StructField(field._1, toSparkType(InternalTypeSystem.TYPE_SYSTEM,field._2), nullable = true) )
    val schema = StructType(fields)
    val rowRdd = CypherRowRDD.apply(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, Any]) = {
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val session = GraphDatabase.driver(config.url).session()

    val result = session.run(query,parameters.asScala.mapValues( Values.value ).asJava)
    if (!result.next()) throw new RuntimeException("Can't determine schema from empty result")

    val fields = result.keys().asScala.map( k => StructField(k, toSparkType(InternalTypeSystem.TYPE_SYSTEM, result.get(k).`type`() )))
    val schema = StructType(fields)
    val rowRdd = CypherRowRDD.apply(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def toSparkType(typeSystem : TypeSystem, typ : Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN) types.BooleanType
    else if (typ == typeSystem.STRING) types.StringType
    else if (typ == typeSystem.INTEGER()) types.LongType
    else if (typ == typeSystem.FLOAT) types.DoubleType
    else if (typ == typeSystem.NULL) types.NullType
    else types.StringType
//    type match {
//    case typeSystem.MAP => types.MapType(types.StringType,types.ObjectType)
//    case typeSystem.LIST => types.ArrayType(types.ObjectType, containsNull = false)
//    case typeSystem.NODE => types.VertexType
//    case typeSystem.RELATIONSHIP => types.EdgeType
//    case typeSystem.PATH => types.GraphType
//    case _ => types.StringType
//  }
}

package org.neo4j.spark

import java.util.NoSuchElementException

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, types}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1._

import scala.collection.JavaConverters._

class CypherResultRdd(@transient sc: SparkContext, result : ResultCursor, session: Session)
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
          val res = if (keyCount == 0) Row.empty
          else if (keyCount == 1) return Row(record.get(0).asObject())
          else {
            val builder = Seq.newBuilder[AnyRef]
            builder.sizeHint(keyCount)
            var i = 0
            while (i < keyCount) {
              builder += record.get(i).asObject()
              i = i + 1
            }
            Row.fromSeq(builder.result())
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

object CypherDataFrame {
  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, Any], schema: (String, types.DataType)*) = {

    val fields = schema.map(field =>
      StructField(field._1, field._2, nullable = true) )
    val rowRdd = CypherRowRDD.apply(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, StructType(fields))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, Any], schemaInfo: Array[(String, String)]) = {

    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherType(field._2), nullable = true) )
    val schema = StructType(fields)
    val rowRdd = CypherRowRDD.apply(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, Any]) = {
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val session = GraphDatabase.driver(config.url).session()

    val params = if (parameters==null) Map[String,Value]().asJava else parameters.asScala.mapValues( Values.value ).asJava
    val result = session.run(query,params)
    if (!result.next()) throw new RuntimeException("Can't determine schema from empty result")

    val fields = result.keys().asScala.map( k => StructField(k, toSparkType(InternalTypeSystem.TYPE_SYSTEM, result.get(k).`type`() )))
    val schema = StructType(fields)
    val rowRdd = new CypherResultRdd(sqlContext.sparkContext, result, session)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def toSparkType(typeSystem : TypeSystem, typ : Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN) CypherType.BOOLEAN
    else if (typ == typeSystem.STRING) CypherType.STRING
    else if (typ == typeSystem.INTEGER()) CypherType.INTEGER
    else if (typ == typeSystem.FLOAT) CypherType.FlOAT
    else if (typ == typeSystem.NULL) CypherType.NULL
    else CypherType.STRING
//    type match {
//    case typeSystem.MAP => types.MapType(types.StringType,types.ObjectType)
//    case typeSystem.LIST => types.ArrayType(types.ObjectType, containsNull = false)
//    case typeSystem.NODE => types.VertexType
//    case typeSystem.RELATIONSHIP => types.EdgeType
//    case typeSystem.PATH => types.GraphType
//    case _ => types.StringType
//  }
}
object CypherType {
  val INTEGER = types.LongType
  val FlOAT = types.DoubleType
  val STRING = types.StringType
  val BOOLEAN = types.BooleanType
  val NULL = types.NullType

  def apply(typ:String) = typ match {
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }
//  val MAP = types.MapType(types.StringType,types.AnyDataType)
//  val LIST = types.ArrayType(types.AnyDataType)
// , MAP, LIST, NODE, RELATIONSHIP, PATH
}

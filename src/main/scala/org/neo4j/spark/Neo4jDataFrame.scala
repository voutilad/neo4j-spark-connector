package org.neo4j.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, types}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.types.{Type, TypeSystem}

import scala.collection.JavaConverters._

object Neo4jDataFrame {

  def schemaFromNamedType(schemaInfo: Seq[(String, String)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherType(field._2), nullable = true) )
    StructType(fields)
  }
  def schemaFromDataType(schemaInfo: Seq[(String, types.DataType)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, field._2, nullable = true) )
    StructType(fields)
  }

  def withDataType(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, types.DataType)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, schemaFromDataType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, String)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, schemaFromNamedType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, AnyRef]) = {
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val driver: Driver = Neo4jConfig.driver(config.url)
    val session = driver.session()

    val result = session.run(query,parameters)
    if (!result.hasNext) throw new RuntimeException("Can't determine schema from empty result")
    val peek = result.peek()
    val fields = peek.keys().asScala.map( k => StructField(k, toSparkType(InternalTypeSystem.TYPE_SYSTEM, peek.get(k).`type`() )))
    val schema = StructType(fields)
    val rowRdd = new Neo4jResultRdd(sqlContext.sparkContext, result.asScala, peek.size(), session, driver)
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
class Neo4jResultRdd(@transient sc: SparkContext, result : Iterator[Record], keyCount : Int, session: Session, driver:Driver)
  extends RDD[Row](sc, Nil)
    with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    result.map((record) => {
      val res = keyCount match {
        case 0 => Row.empty
        case 1 => Row(record.get(0).asObject())
        case _ =>
          val builder = Seq.newBuilder[AnyRef]
          builder.sizeHint(keyCount)
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

}
object CypherType {
  val INTEGER = types.LongType
  val FlOAT = types.DoubleType
  val STRING = types.StringType
  val BOOLEAN = types.BooleanType
  val NULL = types.NullType

  def apply(typ:String) = typ.toUpperCase match {
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }
//  val MAP = types.MapType(types.StringType,types.AnyDataType)
//  val LIST = types.ArrayType(types.AnyDataType)
// , MAP, LIST, NODE, RELATIONSHIP, PATH
}

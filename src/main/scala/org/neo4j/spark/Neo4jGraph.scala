package org.neo4j.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * @author mh
  * @since 06.03.16
  */
object Neo4jGraph {

  // nodeStmt: MATCH (n:Label) RETURN id(n) as id UNION MATCH (m:Label2) return id(m) as id
  // relStmt: MATCH (n:Label1)-[r:REL]->(m:Label2) RETURN id(n), id(m), r.foo // or id(r) or type(r) or ...
  def loadGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, nodeStmt: (String,Seq[(String, Any)]), relStmt: (String,Seq[(String, Any)])) :Graph[VD,ED] = {
    val nodes: RDD[(VertexId, VD)] =
      sc.makeRDD(execute(sc,nodeStmt._1,nodeStmt._2).rows.toSeq)
      .map(row => (row(0).asInstanceOf[Long],row(1).asInstanceOf[VD]))
    val rels: RDD[Edge[ED]] =
      sc.makeRDD(execute(sc,relStmt._1,relStmt._2).rows.toSeq)
      .map(row => new Edge[ED](row(0).asInstanceOf[VertexId],row(1).asInstanceOf[VertexId],row(2).asInstanceOf[ED]))
    Graph[VD,ED](nodes, rels)
  }

  def loadGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, nodeStmt: String, relStmt: String) :Graph[VD,ED] = {
    loadGraph(sc, (nodeStmt,Seq.empty),(relStmt,Seq.empty))
  }

  // label1, label2, relTypes are optional
  // MATCH (n:${label(label1}})-[via:${rels(relTypes)}]->(m:${label(label2)}) RETURN id(n) as from, id(m) as to
  def loadGraph(sc: SparkContext, label1: String, relTypes: Seq[String], label2: String) : Graph[Any,Int] = {
    def label(l : String) = if (l == null) "" else ":`"+l+"`"
    def rels(relTypes : Seq[String]) = relTypes.map(":`"+_+"`").mkString("|")

    val relStmt = s"MATCH (n${label(label1)})-[via${rels(relTypes)}]->(m${label(label2)}) RETURN id(n) as from, id(m) as to"

    loadGraphFromNodePairs[Any](sc,relStmt)
  }

  // MATCH (..)-[r:....]->(..) RETURN id(startNode(r)), id(endNode(r)), r.foo
  def loadGraphFromRels[VD:ClassTag,ED:ClassTag](sc: SparkContext, statement: String, parameters: Seq[(String, Any)], defaultValue : VD = Nil) :Graph[VD,ED] = {
    val rels =
      sc.makeRDD(execute(sc, statement, parameters).rows.toSeq)
        .map(row => new Edge[ED](row(0).asInstanceOf[VertexId], row(1).asInstanceOf[VertexId],row(2).asInstanceOf[ED]))
      Graph.fromEdges[VD,ED](rels, defaultValue)
  }

  // MATCH (..)-[r:....]->(..) RETURN id(startNode(r)), id(endNode(r))
  def loadGraphFromNodePairs[VD:ClassTag](sc: SparkContext, statement: String, parameters: Seq[(String, Any)] = Seq.empty, defaultValue : VD = Nil) :Graph[VD, Int] = {
    val rels: RDD[(VertexId, VertexId)] =
      sc.makeRDD(execute(sc,statement,parameters).rows.toSeq)
        .map(row => (row(0).asInstanceOf[Long],row(1).asInstanceOf[Long]))
    Graph.fromEdgeTuples[VD](rels, defaultValue = defaultValue)
  }

  def saveGraph[VD:ClassTag,ED:ClassTag](sc: SparkContext, graph: Graph[VD,ED], nodeProp : String = null, relProp: String = null) : (Long,Long) = {
    val config = Neo4jConfig(sc.getConf)
    val nodesUpdated : Long = nodeProp match {
      case null => 0
      case _ =>
        val updateNodes = s"UNWIND {data} as row MATCH (n) WHERE id(n) = row.id SET n.$nodeProp = row.value return count(*)"
        val batchSize = ((graph.vertices.count() / 100) + 1).toInt
        graph.vertices.repartition(batchSize).mapPartitions[Long](
          p => {
            // TODO was toIterable instead of toList but bug in java-driver
            val rows: Any = p.map(v => Seq(("id", v._1), ("value", v._2)).toMap.asJava).toList.asJava
            val res1: Iterator[Array[Any]] = execute(config, updateNodes, Seq(("data", rows))).rows
            val sum: Long = res1.map( x => x(0).asInstanceOf[Long]).sum
            Iterator.apply[Long](sum)
          }
        ).sum().toLong
    }

    val relsUpdated : Long = relProp match {
      case null => 0
      case _ =>
        val updateRels = s"UNWIND {data} as row MATCH (n)-[rel]->(m) WHERE id(n) = row.from AND id(m) = row.to SET rel.$relProp = row.value return count(*)"
        val batchSize = ((graph.edges.count() / 100) + 1).toInt

        graph.edges.repartition(batchSize).mapPartitions[Long](
          p => {
            val rows: Any = p.map(e => Seq(("from", e.srcId), ("to", e.dstId), ("value", e.attr))).toList.asJava
            val res1: Iterator[Array[Any]] = execute(config, updateRels, Seq(("data", rows))).rows
            val sum : Long = res1.map(x => x(0).asInstanceOf[Long]).sum
            Iterator.apply[Long](sum)
          }
        ).sum().toLong
    }
    (nodesUpdated,relsUpdated) // todo
  }

  def execute(sc: SparkContext, query: String, parameters: Seq[(String, Any)]): CypherResult = {
    execute(Neo4jConfig(sc.getConf), query, parameters)
  }
  def execute(config: BoltConfig, query: String, parameters: Seq[(String, Any)]): CypherResult = {
    val driver: Driver = GraphDatabase.driver(config.url)
    val session = driver.session()

    val params = parameters.toMap.mapValues(Values.value).asJava
    val result = session.run(query, params)
    if (!result.next()) {
      session.close()
      driver.close()
      return new CypherResult(Vector.empty, Iterator.empty)
    }

    val keyCount = result.size()
    val it = new Iterator[Array[Any]]() {
      var hasNext: Boolean = true

      def recordToArray(record : Record) : Array[Any] = keyCount match {
        case 0 => Array.empty[Any]
        case 1 => Array(record.get(0).asObject())
        case 2 => Array(record.get(0).asObject(), record.get(1).asObject())
        case 3 => Array(record.get(0).asObject(), record.get(1).asObject(), record.get(2).asObject())
        case _ =>
          val array = new Array[Any](keyCount)
          var i = 0
          while (i < keyCount) {
            array.update(i, record.get(i).asObject())
            i = i + 1
          }
          array
      }

      override def next(): Array[Any] = {
        if (hasNext) {
          val res = recordToArray(result.record())
          hasNext = result.next()
          if (!hasNext) {
            session.close()
            driver.close()
          }
          res
        } else throw new NoSuchElementException
      }
    }
    new CypherResult(result.keys().asScala.toVector, it)
  }
}

class CypherResult(val cols: IndexedSeq[String], val rows: Iterator[Array[Any]])

package org.neo4j.spark.reader

import java.time.{LocalDate, LocalDateTime, OffsetTime, ZoneOffset, ZonedDateTime}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.internal.{InternalPoint2D, InternalPoint3D}
import org.neo4j.driver.{Record, Session}
import org.neo4j.spark.{DriverCache, Neo4jOptions, Neo4jQuery}
import java.sql.Timestamp

import org.neo4j.spark.util.Neo4jUtil

import collection.JavaConverters._

class Neo4jInputPartitionReader(private val options: Neo4jOptions,
                                private val schema: StructType,
                                private val jobId: String) extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  var customIterator: Iterator[Record] = _
  var session: Session = _
  var driverCache: DriverCache = new DriverCache(options.connection, jobId)

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options, schema, jobId)

  def next: Boolean = {
    if (customIterator == null) {
      session = driverCache.getOrCreate().session()
      // @todo use readTransaction
      customIterator = session.run(Neo4jQuery.build(options.query)).list.asScala.iterator
    }

    customIterator.hasNext
  }

  def convertFromSpark(value: Any): Any = value match {
    case m: ZonedDateTime => new Timestamp(DateTimeUtils.fromUTCTime(m.toInstant.toEpochMilli, m.getZone.getId))
    case m: LocalDateTime => new Timestamp(DateTimeUtils.fromUTCTime(m.toInstant(ZoneOffset.UTC).toEpochMilli, "UTC"))
    case localDate: LocalDate => localDate.toEpochDay.toInt
    case m: OffsetTime => new Timestamp(m.atDate(LocalDate.ofEpochDay(0)).toInstant.toEpochMilli)
    case m: Long => m.intValue()
    case m: InternalPoint2D =>
      val srid: Integer = m.srid()
      InternalRow.fromSeq(Seq(srid, m.x(), m.y(), m.z()))
    case m: InternalPoint3D =>
      val srid: Integer = m.srid()
      InternalRow.fromSeq(Seq(srid, m.x(), m.y(), m.z()))
    case m: java.util.List[Any] => ArrayData.toArrayData(m.asScala.map(convertFromSpark).toArray)
    case m: String => UTF8String.fromString(value.toString)
    case _ => value
  }

  def get: InternalRow = {
    val record = customIterator.next().get("n").asNode().asMap()
    InternalRow.fromSeq(schema.map(field => {
      convertFromSpark(record.get(field.name))
    }))
  }

  def close(): Unit = {
    Neo4jUtil.closeSafety(session)
    driverCache.close()
  }

}
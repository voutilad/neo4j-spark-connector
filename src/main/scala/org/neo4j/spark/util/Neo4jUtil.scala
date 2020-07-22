package org.neo4j.spark.util

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAmount
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, OffsetTime, Period, ZoneOffset, ZonedDateTime}
import java.util.GregorianCalendar

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.{Session, Transaction}
import org.neo4j.driver.internal.{InternalIsoDuration, InternalPoint2D, InternalPoint3D}
import org.neo4j.spark.service.SchemaService

import collection.JavaConverters._

object Neo4jUtil {

  def closeSafety(autoCloseable: AutoCloseable): Unit = {
    if (autoCloseable == null) {
      return Unit
    }

    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case _ => autoCloseable.close()
      }
    } catch {
      case _ => throw new Exception("This exception should be logged") // @todo Log
    }
  }

  def convertFromNeo4j(value: Any): Any = value match {
    case d: InternalIsoDuration => {
      val months: Integer = d.months().toInt
      val days: Integer = d.days().toInt
      val nanoseconds: Integer = d.nanoseconds()
      val seconds: Integer = d.seconds().toInt
      InternalRow.fromSeq(Seq(UTF8String.fromString(d.toString), months, days, seconds, nanoseconds))
    }
    case lt: LocalTime => UTF8String.fromString(lt.format(DateTimeFormatter.ISO_TIME))
    case dt: ZonedDateTime => new Timestamp(DateTimeUtils.fromUTCTime(dt.toInstant.toEpochMilli, dt.getZone.getId))
    case dt: LocalDateTime => new Timestamp(DateTimeUtils.fromUTCTime(dt.toInstant(ZoneOffset.UTC).toEpochMilli, "UTC"))
    case d: LocalDate => d.toEpochDay.toInt
    case t: OffsetTime => new Timestamp(t.atDate(LocalDate.ofEpochDay(0)).toInstant.toEpochMilli)
    case i: Long => i.intValue()
    case p: InternalPoint2D => {
      val srid: Integer = p.srid()
      InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_2D), srid, p.x(), p.y(), null))
    }
    case p: InternalPoint3D => {
      val srid: Integer = p.srid()
      InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_3D), srid, p.x(), p.y(), p.z()))
    }
    case l: java.util.List[Any] => ArrayData.toArrayData(l.asScala.map(convertFromNeo4j).toArray)
    case s: String => UTF8String.fromString(s)
    case _ => value
  }
}

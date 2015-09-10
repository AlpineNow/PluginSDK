/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.features

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

/**
 * Interface used to define a parser,
 * which must provide methods for taking
 * a data type to and from String representation.
 */
trait DataParser[A] {

  def fromString(s: String): A

  def isValidString(s: String): Boolean

  // Should only be called with Any of type A, but because we can't cast to a generic type at compile time, we use Any here.
  def stringify(a: Any) = a.toString

  // We could add
  // def fromStringSafely(s: String): scala.util.Try[A]

}

trait ParserWithTryCatch[A] extends DataParser[A] {
  override def isValidString(s: String): Boolean = {
    // I know we could use scala.util.Try, but I want to avoid the object creation.
    try {
      fromString(s)
      true
    }
    catch {
      case _: Exception => false
    }
  }
}

object DoubleParser extends ParserWithTryCatch[Double] {
  override def fromString(s: String): Double = s.toDouble
}

object IntParser extends ParserWithTryCatch[Int] {
  override def fromString(s: String): Int = s.toInt
}

object LongParser extends ParserWithTryCatch[Long] {
  override def fromString(s: String): Long = s.toLong
}

object BooleanParser extends ParserWithTryCatch[Boolean] {
  override def fromString(s: String): Boolean = s.toBoolean
}

object StringParser extends DataParser[String] {
  override def fromString(s: String): String = s

  override def isValidString(s: String): Boolean = true
}

case class DateTimeParser(format: Option[String] = None) extends ParserWithTryCatch[DateTime] {

  val formatter: DateTimeFormatter = format match {
    case Some(f) => DateTimeFormat.forPattern(f)
    case None =>
      // Consistent with the Pig ToDate function (lenient parsing)
      // http://grepcode.com/file/repo1.maven.org/maven2/org.apache.pig/pig/0.12.0/org/apache/pig/builtin/ToDate.java#ToDate.0isoDateTimeFormatter
      ISODateTimeFormat.dateOptionalTimeParser.withOffsetParsed
  }

  override def fromString(s: String): DateTime = formatter.parseDateTime(s)
}

object MapStringDoubleParser extends ParserWithTryCatch[java.util.Map[String, java.lang.Double]] {
  val t = new TypeToken[java.util.HashMap[String, java.lang.Double]]() {}.getType
  val gson = new GsonBuilder().serializeSpecialFloatingPointValues.create
  override def fromString(s: String): java.util.HashMap[String, java.lang.Double] = {
    gson.fromJson(s, t)
  }

  override def stringify(a: Any): String = {
    gson.toJson(a)
  }
}

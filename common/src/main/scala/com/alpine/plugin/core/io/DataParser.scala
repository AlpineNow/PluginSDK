/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.plugin.core.io

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
  def stringify(a: Any): String = a.toString

}

object ParserFactory {
  def getParser(pluginType: ColumnType.TypeValue, format: Option[String] = None): DataParser[_] = {
    pluginType match {
      case ColumnType.String => StringParser
      case ColumnType.Int => IntParser
      case ColumnType.Long => LongParser
      case ColumnType.Float => FloatParser
      case ColumnType.Double => DoubleParser
      case ColumnType.Boolean => BooleanParser
      case ColumnType.DateTime => DateTimeParser(format)
      case ColumnType.Sparse => MapStringDoubleParser
    }
  }
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

object FloatParser extends ParserWithTryCatch[Float] {
  override def fromString(s: String): Float = s.toFloat
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
  private val t = new TypeToken[java.util.HashMap[String, java.lang.Double]]() {}.getType
  private val gson = new GsonBuilder().serializeSpecialFloatingPointValues.create
  override def fromString(s: String): java.util.HashMap[String, java.lang.Double] = {
    gson.fromJson(s, t)
  }

  override def stringify(a: Any): String = {
    gson.toJson(a)
  }
}

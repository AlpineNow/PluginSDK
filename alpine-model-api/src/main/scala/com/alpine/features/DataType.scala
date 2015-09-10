/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.features

import org.joda.time.DateTime

/**
 * These are the predefined types of features that can be used with row models.
 * To define a new type, the developer simply has to define the corresponding DataParser,
 * providing logic to convert to and from String.
 *
 * Note that because the type itself is serialized and instantiated by reflection with Gson,
 * it is better to use case class than case object, even if the type has no fields. This is because
 * Gson does not correctly instantiate case objects as singletons, and so equality comparison does
 * work.
 */
trait DataType[A] {
  def getParser(format: Option[String] = None): DataParser[A]
}

case class DoubleType() extends DataType[Double] {
  override def getParser(format: Option[String]) = DoubleParser
}

case class DateTimeType() extends DataType[DateTime] {
  override def getParser(format: Option[String]) = new DateTimeParser(format)
}

case class IntType() extends DataType[Int] {
  override def getParser(format: Option[String]) = IntParser
}

case class LongType() extends DataType[Long] {
  override def getParser(format: Option[String]) = LongParser
}

case class BooleanType() extends DataType[Boolean] {
  override def getParser(format: Option[String]) = BooleanParser
}

case class StringType() extends DataType[String] {
  override def getParser(format: Option[String]) = StringParser
}

case class SparseType() extends DataType[java.util.Map[String, java.lang.Double]] {
  override def getParser(format: Option[String]) = MapStringDoubleParser
}
/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

object ColumnType {

  val SPARK_SQL_DATE_FORMAT = "yyyy-mm-dd"
  val SPARK_SQL_TIME_STAMP_FORMAT = "yyyy-mm-dd hh:mm:ss"
  val PIG_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"

  //add another field for format. Which we can use for the date time format
  case class TypeValue(name: String, format : Option[String]) {
    def this(name: String) {
      this(name, None)
    }
  }


  object TypeValue{
    def apply(name: String): TypeValue = new TypeValue(name)
  }

  // These only represent column types of HDFS based datasets.
  // DB column types are too diverse and many to handle them here.
  val String = TypeValue("String")
  val Int = TypeValue("Int")
  val Long = TypeValue("Long")
  val Float = TypeValue("Float")
  val Double = TypeValue("Double")
  val DateTime = TypeValue("DateTime", Some(PIG_DATE_FORMAT))

  def DateTime(format: String): TypeValue = TypeValue(DateTime.name, Some(format))
  /**
   * Map of Strings to Doubles, serialized in standard JSON.
   */
  val Sparse = TypeValue("Sparse")


}

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
case class ColumnDef(columnName: String, columnType: ColumnType.TypeValue)


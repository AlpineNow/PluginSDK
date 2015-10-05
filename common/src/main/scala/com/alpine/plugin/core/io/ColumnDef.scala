/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

object ColumnType {
  case class TypeValue(name: String)

  // These only represent column types of HDFS based datasets.
  // DB column types are too diverse and many to handle them here.
  val String = TypeValue("String")
  val Int = TypeValue("Int")
  val Long = TypeValue("Long")
  val Float = TypeValue("Float")
  val Double = TypeValue("Double")
  val DateTime = TypeValue("DateTime")
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

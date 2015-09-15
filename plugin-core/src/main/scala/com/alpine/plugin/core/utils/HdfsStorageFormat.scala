/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.utils

object HdfsStorageFormat extends Enumeration {
  type HdfsStorageFormat = Value
  val Parquet, Avro, TSV = Value
}

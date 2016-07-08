/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 *
 * This is unused. The salient information is instead passed along in the HdfsTabularDataset instance at runtime.
 */
@AlpineSdkApi
@deprecated
object TabularFormat extends Enumeration {
  type TabularFormat = Value
  val DelimitedText = Value("DelimitedText")
  val Parquet = Value("Parquet")
  val Avro = Value("Avro")
  val Hive = Value("Hive")
}

/**
 * :: AlpineSdkApi ::
 * @param format The format of this tabular dataset.
 * @param attributes Format specific attributes are stored in this (delimiter,
 *                   escape char, quote char, etc.).
 */
@AlpineSdkApi
@deprecated
case class TabularFormatAttributes(
  format: TabularFormat.TabularFormat,
  attributes: Map[String, String]
)

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
@deprecated
object TabularFormatAttributes {
  def createDelimitedFormat(
    delimiter: String,
    escapeStr: String,
    quoteStr: String
  ): TabularFormatAttributes = {
    TabularFormatAttributes(
      TabularFormat.DelimitedText,
      Map[String, String](
        "delimiter" -> delimiter,
        "escapeStr" -> escapeStr,
        "quoteStr" -> quoteStr
      )
    )
  }
  
  def createTSVFormat(): TabularFormatAttributes = {
    createDelimitedFormat("\t", "\\", "\"")
  }

  def createParquetFormat(): TabularFormatAttributes = {
    TabularFormatAttributes(
      TabularFormat.Parquet,
      Map[String, String]()
    )
  }

  def createAvroFormat(): TabularFormatAttributes = {
    TabularFormatAttributes(
      TabularFormat.Avro,
      Map[String, String]()
    )
  }

  def createHiveFormat(): TabularFormatAttributes = {
    TabularFormatAttributes(
      TabularFormat.Hive,
      Map[String, String]()
    )
  }
}
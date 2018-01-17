/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
trait HdfsDelimitedTabularDataset extends HdfsTabularDataset {
  def tsvAttributes: TSVAttributes
}

case class TSVAttributes(delimiter: Char,
                         escapeStr: Char,
                         quoteStr: Char,
                         containsHeader: Boolean,
                         nullString: String) extends Serializable {

  /*
   This is the map that corresponds to the options for CSV in the DataFrameWriter
   */
  @transient lazy val toMap: Map[String, String] = Map(
    "sep" -> delimiter.toString,
    "escape" -> escapeStr.toString,
    "quote" -> quoteStr.toString,
    "header" -> containsHeader.toString,
    "nullValue" -> nullString
  )

}

object TSVAttributes extends Serializable {
  val DEFAULT_ESCAPE_CHAR = '\\'
  val DEFAULT_QUOTE_CHAR = '\"'
  val TAB_DELIM = '\t'
  val COMMA_DELIM = ','
  val DEFAULT_NULL_STRING = ""

  def apply(delimiter: Char,
    escapeStr: Char,
    quoteStr: Char,
    containsHeader: Boolean): TSVAttributes =
    new TSVAttributes(delimiter, escapeStr, quoteStr, containsHeader, DEFAULT_NULL_STRING)

  /**
    * Reading CSV has more options than writing. This map contains those options.
    * These are specified in org.apache.spark.sql.DataFrameReader
    */
  val additionalReadOptions = Map[String, String](
    "mode" -> "DROPMALFORMED", //PERMISSIVE means that fields are set to null if a corrupt record is met.
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true")

  val defaultCSV =
     TSVAttributes(COMMA_DELIM, DEFAULT_ESCAPE_CHAR, DEFAULT_QUOTE_CHAR, containsHeader = false,
      DEFAULT_NULL_STRING)

  val defaultTSV = TSVAttributes(TAB_DELIM, DEFAULT_ESCAPE_CHAR, DEFAULT_QUOTE_CHAR, containsHeader = false,
    DEFAULT_NULL_STRING)
}

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
                         nullString: String = "") extends Serializable {
  @transient lazy val toMap: Map[String, String] = Map(
    "delimiter" -> delimiter.toString,
    "escape" -> escapeStr.toString,
    "quote" -> quoteStr.toString,
    "header" -> containsHeader.toString,
    "nullValue" -> nullString
  )

}

object TSVAttributes extends Serializable {
  val DEFAULT_ESCAPE_CHAR = '\\'
  val DEFAULT_QUOTE_CHAR = '\"'
  val DEFAULT_DELIM = '\t'
  val COMMA_DELIM = ','

  val default =
    TSVAttributes(DEFAULT_DELIM, DEFAULT_ESCAPE_CHAR, DEFAULT_QUOTE_CHAR, containsHeader = false)

  val defaultCSV =
    TSVAttributes(COMMA_DELIM, DEFAULT_ESCAPE_CHAR, DEFAULT_QUOTE_CHAR, containsHeader = false)
}

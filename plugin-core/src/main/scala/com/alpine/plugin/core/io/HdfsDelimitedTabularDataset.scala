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
                         containsHeader: Boolean)

object TSVAttributes {
  val default = TSVAttributes('\t', '\\', '\"', containsHeader = false)
}

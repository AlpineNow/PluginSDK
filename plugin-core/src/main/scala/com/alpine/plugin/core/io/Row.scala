/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * Used to represent a row in a local table.
 * A row in a table is simply a list of strings.
 */
@AlpineSdkApi
case class Row(values: Seq[String]) {
  def getNumCols: Int = {
    values.length
  }

  def valuesAsArray: Array[String] = values.toArray
}

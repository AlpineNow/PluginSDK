/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * Local in-memory table for storing small tabular data.
 * This can be used to store things like confusion matrix or a small data sample
 * for a large dataset.
 */
@AlpineSdkApi
trait LocalTable extends IOBase {

  /**
   * Get the number of columns in the table.
   * @return The number of columns.
   */
  def getNumCols: Int

  def getNumRows: Int

  /**
   * Get an iterator of all the rows.
   * @return An iterator of all the rows.
   */
  def rows: Seq[Row]
}

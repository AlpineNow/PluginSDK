/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{LocalTable, OperatorInfo, Row}

/**
  * Abstract implementation of [[LocalTable]].
  * Can be extended by developers who want custom behaviour not provided by [[LocalTable]].
  * @param tableName The name used to display this object in the UI.
  * @param rows The content of the table.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractLocalTable(val tableName: String, val rows: Seq[Row],
                                  val sourceOperatorInfo: Option[OperatorInfo],
                                  val addendum: Map[String, AnyRef])
  extends LocalTable {

  /**
   * Get the number of columns in the table.
   * @return The number of columns.
   */
  def getNumCols: Int = {
    if (this.rows.nonEmpty) {
      rows.head.getNumCols
    } else {
      0
    }
  }

  /**
   * Get the number of rows in the table.
   * @return The number of rows.
   */
  def getNumRows: Int = this.rows.length

  def displayName: String = tableName

}

/**
  * Default implementation of [[LocalTable]].
  * @param tableName The name used to display this object in the UI.
  * @param rows The content of the table.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class LocalTableDefault(override val tableName: String,
                             override val rows: Seq[Row],
                             override val sourceOperatorInfo: Option[OperatorInfo],
                             override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractLocalTable(tableName, rows, sourceOperatorInfo, addendum)
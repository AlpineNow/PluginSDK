/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HiveTable, OperatorInfo, TabularSchema}

/**
  * Abstract implementation of [[HiveTable]].
  * Can be extended by developers who want custom behaviour not provided by [[HiveTableDefault]].
  *
  * @param tableName Name of the table.
  * @param dbName Name of the database (sometimes referred to as "schema") containing the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractHiveTable(
  val tableName: String,
  val dbName: Option[String],
  val tabularSchema: TabularSchema,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef]
) extends HiveTable {
  def getConcatenatedName: String = HiveTable.getConcatenatedName(tableName, dbName)
  def displayName: String = getConcatenatedName
}

/**
  * Default implementation of [[HiveTable]].
  * @param tableName Name of the table.
  * @param dbName Name of the database (sometimes referred to as "schema") containing the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class HiveTableDefault(
  override val tableName: String,
  override val dbName: Option[String],
  override val tabularSchema: TabularSchema,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends AbstractHiveTable(tableName, dbName, tabularSchema, sourceOperatorInfo, addendum)
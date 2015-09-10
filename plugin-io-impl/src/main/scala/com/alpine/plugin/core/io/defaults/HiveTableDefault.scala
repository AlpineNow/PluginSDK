/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HiveTable, OperatorInfo, TabularSchema}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HiveTable.
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
case class HiveTableDefault(
  override val tableName: String,
  override val dbName: Option[String],
  override val tabularSchema: TabularSchema,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends AbstractHiveTable(tableName, dbName, tabularSchema, sourceOperatorInfo, addendum)
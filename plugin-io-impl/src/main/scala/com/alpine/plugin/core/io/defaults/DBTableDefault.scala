/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{DBTable, OperatorInfo, TabularSchema}

/**
 * Default implementation is DBTableDefault.
 * Developers wanting to change behaviour can extend AbstractDBTable.
 */
abstract class AbstractDBTable(val schemaName: String,
                               val tableName: String,
                               val tabularSchema: TabularSchema,
                               val isView: Boolean,
                               val dbName: String,
                               val dbURL: String,
                               val sourceOperatorInfo: Option[OperatorInfo],
                               val addendum: Map[String, AnyRef])
  extends DBTable {
  override def displayName: String = tableName
}

case class DBTableDefault(override val schemaName: String,
                          override val tableName: String,
                          override val tabularSchema: TabularSchema,
                          override val isView: Boolean,
                          override val dbName: String,
                          override val dbURL: String,
                          override val sourceOperatorInfo: Option[OperatorInfo],
                          override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractDBTable(schemaName, tableName, tabularSchema, isView, dbName, dbURL, sourceOperatorInfo, addendum)

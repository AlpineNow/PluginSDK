/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HiveTable, TabularSchema}

/**
  * Abstract implementation of [[HiveTable]].
  * Can be extended by developers who want custom behaviour not provided by [[HiveTableDefault]].
  *
  * @param tableName     Name of the table.
  * @param dbName        Name of the database (sometimes referred to as "schema") containing the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum      Map containing additional information.
  */
abstract class AbstractHiveTable(val tableName: String,
                                 val dbName: Option[String],
                                 val tabularSchema: TabularSchema,
                                 val addendum: Map[String, AnyRef]
                                ) extends HiveTable {
  def getConcatenatedName: String = HiveTable.getConcatenatedName(tableName, dbName)
}

/**
  * Default implementation of [[HiveTable]].
  *
  * @param tableName     Name of the table.
  * @param dbName        Name of the database (sometimes referred to as "schema") containing the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum      Map containing additional information.
  */
case class HiveTableDefault(override val tableName: String,
                            override val dbName: Option[String],
                            override val tabularSchema: TabularSchema,
                            override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
                           ) extends AbstractHiveTable(tableName, dbName, tabularSchema, addendum)
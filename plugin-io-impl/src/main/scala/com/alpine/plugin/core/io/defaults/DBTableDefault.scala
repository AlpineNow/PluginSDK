/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{DBTable, OperatorInfo, TabularSchema}

/**
  * Abstract implementation of [[DBTable]].
  * Can be extended by developers who want custom behaviour not provided by [[DBTableDefault]].
  *
  * @param schemaName    Name of the schema containing the table.
  * @param tableName     Name of the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param isView        Boolean indicating whether this is a view (not materialised). Otherwise, taken to be a table.
  * @param dbName        Name of the database.
  * @param dbURL         URL of the database.
  * @param addendum      Map containing additional information.
  */
abstract class AbstractDBTable(val schemaName: String,
                               val tableName: String,
                               val tabularSchema: TabularSchema,
                               val isView: Boolean,
                               val dbName: String,
                               val dbURL: String,
                               val addendum: Map[String, AnyRef])
  extends DBTable

/**
  * Default implementation of [[DBTable]].
  *
  * @param schemaName    Name of the schema containing the table.
  * @param tableName     Name of the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param isView        Boolean indicating whether this is a view (not materialised). Otherwise, taken to be a table.
  * @param dbName        Name of the database.
  * @param dbURL         URL of the database.
  * @param addendum      Map containing additional information.
  */
case class DBTableDefault(override val schemaName: String,
                          override val tableName: String,
                          override val tabularSchema: TabularSchema,
                          override val isView: Boolean,
                          override val dbName: String,
                          override val dbURL: String,
                          override val addendum: Map[String, AnyRef])
  extends AbstractDBTable(schemaName, tableName, tabularSchema, isView, dbName, dbURL, addendum) {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def this(schemaName: String,
           tableName: String,
           tabularSchema: TabularSchema,
           isView: Boolean,
           dbName: String,
           dbURL: String,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()
          ) = {
    this(schemaName, tableName, tabularSchema, isView, dbName, dbURL, addendum)
  }
}

object DBTableDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema,
            isView: Boolean,
            dbName: String,
            dbURL: String,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef] = Map[String, AnyRef]()): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, isView, dbName, dbURL, addendum)
  }

  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema,
            isView: Boolean,
            dbName: String,
            dbURL: String): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, isView, dbName, dbURL, Map[String, AnyRef]())
  }
}

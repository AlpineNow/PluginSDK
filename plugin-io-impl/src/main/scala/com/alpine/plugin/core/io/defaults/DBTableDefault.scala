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
  * @param addendum      Map containing additional information.
  */
abstract class AbstractDBTable(val schemaName: String,
                               val tableName: String,
                               val tabularSchema: TabularSchema,
                               val addendum: Map[String, AnyRef])
  extends DBTable {
  @deprecated
  override def isView: Boolean = false

  @deprecated
  override def dbName: String = ""

  @deprecated
  override def dbURL: String = ""
}

/**
  * Default implementation of [[DBTable]].
  *
  * @param schemaName    Name of the schema containing the table.
  * @param tableName     Name of the table.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum      Map containing additional information.
  */
case class DBTableDefault(override val schemaName: String,
                          override val tableName: String,
                          override val tabularSchema: TabularSchema,
                          override val addendum: Map[String, AnyRef])
  extends AbstractDBTable(schemaName, tableName, tabularSchema, addendum) {

  @deprecated("Use constructor without sourceOperatorInfo.")
  @Deprecated
  def this(schemaName: String,
           tableName: String,
           tabularSchema: TabularSchema,
           isView: Boolean,
           dbName: String,
           dbURL: String,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()
          ) = {
    this(schemaName, tableName, tabularSchema, addendum)
  }

  @deprecated("Use constructor without sourceOperatorInfo.")
  @Deprecated
  def this(schemaName: String,
           tableName: String,
           tabularSchema: TabularSchema,
           isView: Boolean,
           dbName: String,
           dbURL: String,
           addendum: Map[String, AnyRef]) = {
    this(schemaName, tableName, tabularSchema, addendum)
  }
}

object DBTableDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  @Deprecated
  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema,
            isView: Boolean,
            dbName: String,
            dbURL: String,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef] = Map[String, AnyRef]()): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, addendum)
  }

  @deprecated("Use constructor without isView, dbName and dbURL.")
  @Deprecated
  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema,
            isView: Boolean,
            dbName: String,
            dbURL: String,
            addendum: Map[String, AnyRef]): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, addendum)
  }

  @deprecated("Use constructor without isView, dbName and dbURL.")
  @Deprecated
  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema,
            isView: Boolean,
            dbName: String,
            dbURL: String): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, Map[String, AnyRef]())
  }

  def apply(schemaName: String,
            tableName: String,
            tabularSchema: TabularSchema): DBTableDefault = {
    new DBTableDefault(schemaName, tableName, tabularSchema, Map[String, AnyRef]())
  }
}

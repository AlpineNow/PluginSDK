/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.util

import com.alpine.sql.DatabaseType._
import com.alpine.sql.{DatabaseType, SQLGenerator}

/**
  * Provides very basic methods for SQL code generation, based on PostgreSQL.
  * Aside from Test classes, this class should not be instantiated.
  * Instead, use SQLGeneratorFactory.getSQLGenerator or DBExecutionContext.getSQLGenerator
  */
class SimpleSQLGenerator extends SQLGenerator {

  private var databaseType:    TypeValue = postgres
  private var quoteString:     String    = "\""
  private var isAliasRequired: Boolean   = true

  // single arg constructor for DatabaseType
  def this(dbType: TypeValue) = {
    this()
    databaseType = dbType
    if (dbType == DatabaseType.hive) {
      quoteString = "`"
    }
  }

  override def dbType = databaseType

  override def quoteChar: String = quoteString
  override def useAliasForSelectSubQueries: Boolean = isAliasRequired

  @deprecated("Please use quoteIdentifier instead [Paul]", "2016-04-22")
  override def escapeColumnName(s: String): String = quoteIdentifier(s)

  override def quoteIdentifier(s: String): String = quoteString + s + quoteString

  override def quoteObjectName(schemaName: String, objectName: String): String = {
    schemaName match {
      case null | "" => quoteIdentifier(objectName)
      case _         => quoteIdentifier(schemaName) + "." + quoteIdentifier(objectName)
    }
  }

  override def getStandardDeviationFunctionName: String = "stddev"

  override def getVarianceFunctionName: String = "variance"

  override def getModuloExpression(dividend: String, divisor: String): String = dividend + " % " + divisor

  override def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String = {
    s"""CREATE TABLE $destinationTable AS (SELECT $columns FROM $sourceTable $whereClause"""
  }

  override def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String): String = {
    getCreateTableAsSelectSQL(columns, sourceTable, destinationTable, "")
  }

  override def getCreateTableAsSelectSQL(selectQuery: String, destinationTable: String): String = {
    getCreateTableOrViewAsSelectSQL(selectQuery, destinationTable, false)
  }

  override def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String, whereClause: String): String = {
    s"""CREATE VIEW $destinationView AS (SELECT $columns FROM $sourceTable $whereClause"""
  }

  override def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String): String = {
    getCreateViewAsSelectSQL(columns, sourceTable, destinationView, "")
  }

  override def getCreateViewAsSelectSQL(selectQuery: String, destinationView: String): String = {
    getCreateTableOrViewAsSelectSQL(selectQuery, destinationView, true)
  }

  override def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String, isView: Boolean): String = {
    if (isView) {
      getCreateViewAsSelectSQL(columns, sourceTable, destinationTable, whereClause)
    } else {
      getCreateTableAsSelectSQL(columns, sourceTable, destinationTable, whereClause)
    }
  }

  override def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, isView: Boolean): String = {
    if (isView) {
      getCreateViewAsSelectSQL(columns, sourceTable, destinationTable)
    } else {
      getCreateTableAsSelectSQL(columns, sourceTable, destinationTable)
    }
  }

  override def getCreateTableOrViewAsSelectSQL(selectQuery: String, destinationTable: String, isView: Boolean): String = {
    if (isView) {
      s"""CREATE TABLE $destinationTable AS ($selectQuery)"""
    } else {
      s"""CREATE VIEW $destinationTable AS ($selectQuery)"""
    }
  }

  override def getDropTableIfExistsSQL(tableName: String, cascade: Boolean = false) = {
    if (cascade) {
      s"""DROP TABLE IF EXISTS $tableName"""
    } else {
      s"""DROP TABLE IF EXISTS $tableName CASCADE"""
    }
  }

  override def getDropViewIfExistsSQL(viewName: String, cascade: Boolean = false) = {
    if (cascade) {
      s"""DROP VIEW IF EXISTS $viewName"""
    } else {
      s"""DROP VIEW IF EXISTS $viewName CASCADE"""
    }
  }
}

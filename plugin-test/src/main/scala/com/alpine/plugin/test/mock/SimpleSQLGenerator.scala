package com.alpine.plugin.test.mock

import com.alpine.sql.{DatabaseType, SQLGenerator}

class SimpleSQLGenerator(val dbType: DatabaseType.TypeValue = DatabaseType.postgres) extends SQLGenerator {

  private val quoteString: String = {
    if (dbType == DatabaseType.hive) {
      "`"
    } else {
      "\""
    }
  }

  override def quoteChar: String = quoteString

  override def useAliasForSelectSubQueries: Boolean = true

  @deprecated("Please use quoteIdentifier instead [Paul]", "2016-04-22")
  override def escapeColumnName(s: String): String = quoteIdentifier(s)

  override def quoteIdentifier(s: String): String = quoteString + s + quoteString

  override def quoteObjectName(schemaName: String, objectName: String): String = {
    schemaName match {
      case null | "" => quoteIdentifier(objectName)
      case _ => quoteIdentifier(schemaName) + "." + quoteIdentifier(objectName)
    }
  }

  override def getStandardDeviationFunctionName: String = "stddev"

  override def getStandardDeviationPopulationFunctionName: String = "STDDEV_POP"

  override def getStandardDeviationSampleFunctionName: String = "STDDEV_SAMP"

  override def getVarianceFunctionName: String = "variance"

  override def getVariancePopulationFunctionName: String = "VAR_POP"

  override def getVarianceSampleFunctionName: String = "VAR_SAMP"

  override def getCorrelationExpression(columnX: String, columnY: String): String = "CORR(" + columnX + ", " + columnY + ")"

  override def getModuloExpression(dividend: String, divisor: String): String = dividend + " % " + divisor

  override def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String = {
    s"""CREATE TABLE $destinationTable AS (SELECT $columns FROM $sourceTable $whereClause)"""
  }

  override def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String): String = {
    getCreateTableAsSelectSQL(columns, sourceTable, destinationTable, "")
  }

  override def getCreateTableAsSelectSQL(selectQuery: String, destinationTable: String): String = {
    getCreateTableOrViewAsSelectSQL(selectQuery, destinationTable, isView = false)
  }

  override def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String, whereClause: String): String = {
    s"""CREATE VIEW $destinationView AS (SELECT $columns FROM $sourceTable $whereClause"""
  }

  override def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String): String = {
    getCreateViewAsSelectSQL(columns, sourceTable, destinationView, "")
  }

  override def getCreateViewAsSelectSQL(selectQuery: String, destinationView: String): String = {
    getCreateTableOrViewAsSelectSQL(selectQuery, destinationView, isView = true)
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
      s"""CREATE VIEW $destinationTable AS ($selectQuery)"""
    } else {
      s"""CREATE TABLE $destinationTable AS ($selectQuery)"""
    }
  }

  override def getDropTableIfExistsSQL(tableName: String, cascade: Boolean = false): String = {
    if (cascade) {
      s"""DROP TABLE IF EXISTS $tableName"""
    } else {
      s"""DROP TABLE IF EXISTS $tableName CASCADE"""
    }
  }

  override def getDropViewIfExistsSQL(viewName: String, cascade: Boolean = false): String = {
    if (cascade) {
      s"""DROP VIEW IF EXISTS $viewName"""
    } else {
      s"""DROP VIEW IF EXISTS $viewName CASCADE"""
    }
  }

  override def getCreateTempTableAsSelectSQL(selectQuery: String, destinationTable: String): String = {
    s"""CREATE TEMP TABLE $destinationTable AS ($selectQuery)"""
  }

  /**
    * Should one put quotes around boolean values
    *
    * introduced in 2019 mainly to support BigQuery
    *
    * @return Boolean telling whether or not to put quotes around Boollean values
    */
  override def quoteBooleanValues(): Boolean = false

  /**
    * Convert strings read from table into boolean
    *
    * introduced in 2019 mainly to support BigQuery
    *
    * @return Boolean corresponding to the string value
    */
  override def convertStringToBoolean(input: String): Boolean = false
}

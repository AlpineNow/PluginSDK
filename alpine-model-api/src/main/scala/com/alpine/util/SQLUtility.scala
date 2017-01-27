/*
 * Copyright (C) 2016 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.util

import com.alpine.sql.{AliasGenerator, SQLGenerator}
import com.alpine.transformer.sql._

object SQLUtility {

  def comparedToOthers(name: ColumnName, others: Seq[ColumnName], comparator: String, sqlGenerator: SQLGenerator): String = {
    if (others.isEmpty) ""
    else name.escape(sqlGenerator) + " " + comparator + " " + others.head.escape(sqlGenerator) + {
      if (others.size == 1) ""
      else " AND " + comparedToOthers(name, others.tail, comparator, sqlGenerator)
    }
  }

  def wrapInSingleQuotes(s: String): String = "'" + s + "'"

  /**
    * Expect numeric or String.
    * Numeric values need to be used raw (without quotes).
    */
  def wrapAsValue(a: Any): String = {
    a match {
      case s: String => wrapInSingleQuotes(s)
      case _ => a.toString
    }
  }

  def argMinOrMaxSQL(labelValuesToColumnNames: Seq[(String, ColumnName)], comparator: String, sqlGenerator: SQLGenerator): String = {
    def comparisonSQL(columnNamesAndLabelValues: Seq[(String, ColumnName)]): String = {
      if (columnNamesAndLabelValues.size == 1) " ELSE " + wrapInSingleQuotes(columnNamesAndLabelValues.head._1)
      else " WHEN (" + {
        comparedToOthers(columnNamesAndLabelValues.head._2, columnNamesAndLabelValues.tail.map(_._2), comparator, sqlGenerator) +
          ") THEN " + wrapInSingleQuotes(columnNamesAndLabelValues.head._1) +
          comparisonSQL(columnNamesAndLabelValues.tail)
      }
    }
    nullWhenAnyColumnNull(labelValuesToColumnNames.map(_._2), sqlGenerator,"(CASE" + comparisonSQL(labelValuesToColumnNames) + " END)")
  }

  def minOrMaxByRowSQL(columnNames: Seq[ColumnName], comparator: String, sqlGenerator: SQLGenerator): String = {
    def comparisonSQL(columnNamesSubList: Seq[ColumnName]): String = {
      if (columnNamesSubList.size == 1) " ELSE " + columnNamesSubList.head.escape(sqlGenerator)
      else " WHEN (" + {
        comparedToOthers(columnNamesSubList.head, columnNamesSubList.tail, comparator, sqlGenerator) +
          ") THEN " + columnNamesSubList.head.escape(sqlGenerator) +
          comparisonSQL(columnNamesSubList.tail)
      }
    }
    "(CASE" + comparisonSQL(columnNames) + " END)"
  }

  def nullWhenAnyColumnNull(columnNames: Seq[ColumnName], sqlGenerator: SQLGenerator, innards: String): String = {
    s"CASE WHEN ${columnNames.map(_.escape(sqlGenerator) + " IS NULL").mkString(" OR ")} THEN NULL ELSE $innards END"
  }

  def argMinSQL(labelValuesToColumnNames: Seq[(String, ColumnName)], sqlGenerator: SQLGenerator): String = {
    argMinOrMaxSQL(labelValuesToColumnNames, "<", sqlGenerator)
  }

  def argMaxSQL(labelValuesToColumnNames: Seq[(String, ColumnName)], sqlGenerator: SQLGenerator): String = {
    argMinOrMaxSQL(labelValuesToColumnNames, ">", sqlGenerator)
  }

  def groupBySQL(groupByFeature: ColumnarSQLExpression, valuesToColumnNames: Map[ColumnarSQLExpression, ColumnarSQLExpression]): ColumnarSQLExpression = {
    val innards = valuesToColumnNames.map{
      case (value, valueToSelect) => s"""WHEN (${groupByFeature.sql} = ${value.sql}) THEN ${valueToSelect.sql}"""
    }.mkString(" ")
    ColumnarSQLExpression(s"""(CASE $innards ELSE NULL END)""")
  }

  def dotProduct(x: Seq[String], y: Seq[String]): String = {
    (x zip y).map {
      case (xi, yi) => xi + " * " + yi
    }.mkString(" + ")
  }

  def createTable(sql: LayeredSQLExpressions,
                  inputTableName: String,
                  outputTableName: String,
                  aliasGenerator: AliasGenerator,
                  sqlGenerator: SQLGenerator): String = {
    val selectStatement: String = getSelectStatement(sql, inputTableName, aliasGenerator, sqlGenerator)
    s"""CREATE TABLE $outputTableName AS $selectStatement"""
  }

  def createTempTable(sql: LayeredSQLExpressions,
                  inputTableName: String,
                  outputTableName: String,
                  aliasGenerator: AliasGenerator,
                  sqlGenerator: SQLGenerator): String = {
    val selectStatement: String = getSelectStatement(sql, inputTableName, aliasGenerator, sqlGenerator)
    s"""CREATE TEMP TABLE $outputTableName AS $selectStatement"""
  }

  def getSelectStatement(sql: LayeredSQLExpressions,
                         inputTableName: String,
                         aliasGenerator: AliasGenerator,
                         sqlGenerator: SQLGenerator): String = {
    val reversed = sql.layers.reverse
    s"""SELECT ${selectColumnsAs(reversed.head, sqlGenerator)} FROM ${selectFromInput(reversed.tail, inputTableName, aliasGenerator, sqlGenerator)}"""
  }

  def selectFromInput(intermediateLayers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]], input: String, aliasGenerator: AliasGenerator, sqlGenerator: SQLGenerator): String = {
    if (intermediateLayers == Nil) {
      input
    } else {
      s"""(SELECT ${selectColumnsAs(intermediateLayers.head, sqlGenerator)} FROM ${selectFromInput(intermediateLayers.tail, input, aliasGenerator, sqlGenerator)})${
        aliasPhraseIfNeeded(sqlGenerator, aliasGenerator)
      }"""
    }
  }

  def aliasPhraseIfNeeded(sqlGenerator: SQLGenerator, aliasGenerator: AliasGenerator): String = {
    if (sqlGenerator.useAliasForSelectSubQueries) {
      " AS " + aliasGenerator.getNextAlias
    } else ""
  }

  def selectColumnsAs(columns: Seq[(ColumnarSQLExpression, ColumnName)], sqlGenerator: SQLGenerator): String = {
    columns.map { case (expression, name) => expression.sql + " AS " + name.escape(sqlGenerator) }.mkString(", ")
  }
}

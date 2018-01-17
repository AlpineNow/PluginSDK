/*
 * Copyright (C) 2016 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.util

import com.alpine.sql.{AliasGenerator, DatabaseType, SQLGenerator}
import com.alpine.transformer.sql._

object SQLUtility {

  def comparedToOthers(name: ColumnName, others: Seq[ColumnName], comparator: String, sqlGenerator: SQLGenerator): String = {
    others.map(o => name.escape(sqlGenerator) + " " + comparator + " " + o.escape(sqlGenerator)).mkString(" AND ")
  }

  def wrapInSingleQuotes(s: String): String = "'"  + s.replace("'", "''")+  "'"

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
    val builder = new StringBuilder
    var started = false
    val (values, names) = labelValuesToColumnNames.unzip
    var (valuesHead: String, valuesTail: Seq[String]) = (values.head, values.tail)
    var (namesHead: ColumnName, namesTail: Seq[ColumnName]) = (names.head, names.tail)
    // Executes for all but the last case.
    while (namesTail.nonEmpty) {
      if (started) {
        builder.append(" ")
      } else {
        started = true
      }
      val comparisonSQL = comparedToOthers(namesHead, namesTail, comparator, sqlGenerator)
      builder.append(s"WHEN ($comparisonSQL) THEN ${wrapInSingleQuotes(valuesHead)}")
      namesHead = namesTail.head
      namesTail = namesTail.tail
      valuesHead = valuesTail.head
      valuesTail = valuesTail.tail
    }

    val notNullableSQL = s"(CASE $builder ELSE ${wrapInSingleQuotes(valuesHead)} END)"
    nullWhenAnyColumnNull(names, sqlGenerator, notNullableSQL)
  }

  def minOrMaxByRowSQL(columnNames: Seq[ColumnName], comparator: String, sqlGenerator: SQLGenerator): String = {
    var (namesHead: ColumnName, namesTail: Seq[ColumnName]) = (columnNames.head, columnNames.tail)
    val builder = new StringBuilder
    var started = false
    while (namesTail.nonEmpty) {
      if (started) {
        builder.append(" ")
      } else {
        started = true
      }
      val comparisonSQL = comparedToOthers(namesHead, namesTail, comparator, sqlGenerator)
      builder.append(s"WHEN ($comparisonSQL) THEN ${namesHead.escape(sqlGenerator)}")
      namesHead = namesTail.head
      namesTail = namesTail.tail
    }
    builder.append(s" ELSE ${namesHead.escape(sqlGenerator)}")
    "(CASE " + builder + " END)"
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
    val innards = valuesToColumnNames.map {
      case (value, valueToSelect) => s"""WHEN (${groupByFeature.sql} = ${value.sql}) THEN ${valueToSelect.sql}"""
    }.mkString(" ")
    ColumnarSQLExpression(s"""(CASE $innards ELSE NULL END)""")
  }

  def dotProduct(x: Seq[String], y: Seq[String]): String = {
    (x zip y).map {
      case (xi, yi) => xi + " * " + yi
    }.mkString(" + ")
  }

  def createTable(
    sql: LayeredSQLExpressions,
    inputTableName: String,
    outputTableName: String,
    aliasGenerator: AliasGenerator,
    sqlGenerator: SQLGenerator): String = {
    val selectStatement: String = getSelectStatement(sql, inputTableName, aliasGenerator, sqlGenerator)
    sqlGenerator.dbType match {
      // basically retain old functionality just in case
      case DatabaseType.postgres | DatabaseType.greenplum | DatabaseType.oracle => s"""CREATE TABLE $outputTableName AS $selectStatement"""
      case _ => sqlGenerator.getCreateTableAsSelectSQL(selectStatement, outputTableName)
    }
  }

  def createTempTable(
    sql: LayeredSQLExpressions,
    inputTableName: String,
    outputTableName: String,
    aliasGenerator: AliasGenerator,
    sqlGenerator: SQLGenerator): String = {
    val selectStatement: String = getSelectStatement(sql, inputTableName, aliasGenerator, sqlGenerator)
    sqlGenerator.getCreateTempTableAsSelectSQL(selectStatement, outputTableName)
  }

  def getSelectStatement(sql: LayeredSQLExpressions,
    inputTableName: String,
    aliasGenerator: AliasGenerator,
    sqlGenerator: SQLGenerator): String = {
    val reversed = sql.layers.reverse
    s"""SELECT ${selectColumnsAs(reversed.head, sqlGenerator)} FROM ${selectFromInput(reversed.tail, inputTableName, aliasGenerator, sqlGenerator)}"""
  }

  // Recursive, but if our nested select statements are that deep we have a problem.
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

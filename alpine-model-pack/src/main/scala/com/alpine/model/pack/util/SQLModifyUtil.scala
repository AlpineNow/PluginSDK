/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.util

import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression}
import com.facebook.presto.sql.ExpressionFormatter
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree._

object SQLModifyUtil {

  def replaceColumnNames(expression: ColumnarSQLExpression, nameMap: Map[ColumnName, ColumnName], sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    if (sqlGenerator.quoteChar != '"') {
      // E.g. for Hive (Presto doesn't support back-ticks).
      replaceColumnNameNaively(expression, nameMap, sqlGenerator)
    } else {
      replaceColumnNameWithPresto(expression, nameMap)
    }
  }

  private def replaceColumnNameNaively(expression: ColumnarSQLExpression, nameMap: Map[ColumnName, ColumnName], sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    /**
      * There is a hole here - what if the SQL contains the string column name inside single quotes, being used
      * as text in a string?
      * e.g.
      * case when "wind" = 'true' then '"wind"' else '"no wind"' end
      * I think this is a necessary evil.
      */
    val newSQL = nameMap.foldLeft(expression.sql)((sql, tuple) => sql.replaceAllLiterally(tuple._1.escape(sqlGenerator), tuple._2.escape(sqlGenerator)))
    ColumnarSQLExpression(newSQL)
  }

  private def replaceColumnNameWithPresto(expression: ColumnarSQLExpression, nameMap: Map[ColumnName, ColumnName]): ColumnarSQLExpression = {
    val sqlParser = new SqlParser()
    val reWriter = new ExpressionTreeRewriter[String](new ColumnNameChanger(nameMap))
    val parsedExpression = sqlParser.createExpression(expression.sql)
    val stuff = reWriter.rewrite(parsedExpression, "What is this for? I think it's a catch all to enable the user to pass information around.")
    ColumnarSQLExpression(ExpressionFormatter.formatExpression(stuff))
  }

  private class ColumnNameChanger(nameMap: Map[ColumnName, ColumnName]) extends ExpressionRewriter[String] {

    override def rewriteQualifiedNameReference(node: QualifiedNameReference,
                                               context: String,
                                               treeRewriter: ExpressionTreeRewriter[String]
                                              ): Expression = {
      val newName = nameMap.get(ColumnName(node.getName.toString))
      newName match {
        case Some(ColumnName(name)) => new QualifiedNameReference(QualifiedName.parseQualifiedName(name))
        case _ => node
      }
    }
  }
}

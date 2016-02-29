/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.util

import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression}

object SQLModifyUtil {

  def replaceColumnNames(expression: ColumnarSQLExpression, nameMap: Map[ColumnName, ColumnName], sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    replaceColumnNameNaively(expression, nameMap, sqlGenerator)
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

}

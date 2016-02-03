/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.util

import com.alpine.sql.{DatabaseType, SQLGenerator}
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

class SQLModifyUtilTest extends FunSuite {

  import com.alpine.model.pack.util.SQLModifyUtil._

  test("Replacing column names should work") {

    assert(ColumnarSQLExpression("\"column_0\"") ===
      replaceColumnNames(
        ColumnarSQLExpression("\"wind\""),
        Map(ColumnName("wind") -> ColumnName("column_0")),
        new SimpleSQLGenerator
      )
    )

    assert(ColumnarSQLExpression("`column_0`") ===
      replaceColumnNames(
        ColumnarSQLExpression("`wind`"),
        Map(ColumnName("wind") -> ColumnName("column_0")),
        new SQLGenerator {
          override def dbType = DatabaseType.hive

          override def useAliasForSelectSubQueries: Boolean = true

          override def quoteChar: Char = '`'

          override def escapeColumnName(s: String): String = quoteChar + s + quoteChar
        }
      )
    )

    assert("(CASE WHEN (\"column_1\" = 'sunny') THEN 1 ELSE 0 END)" ===
      replaceColumnNames(
        ColumnarSQLExpression("case when \"outlook\" = 'sunny' then 1 else 0 end"),
        Map(ColumnName("outlook") -> ColumnName("column_1")),
        new SimpleSQLGenerator
      ).sql
    )

    assert("(CASE WHEN (\"column_1\" = 'true') THEN '\"wind\"' ELSE 'no \"wind\"' END)" ===
      replaceColumnNames(
        ColumnarSQLExpression("case when \"wind\" = 'true' then '\"wind\"' else 'no \"wind\"' end"),
        Map(ColumnName("wind") -> ColumnName("column_1")),
        new SimpleSQLGenerator
      ).sql
    )

  }

}

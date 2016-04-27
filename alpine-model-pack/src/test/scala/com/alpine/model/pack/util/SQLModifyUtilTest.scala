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
    /* Should preserve case. */
    assert(ColumnarSQLExpression("\"Column_0\" + \"HI\"") ===
      replaceColumnNames(
        ColumnarSQLExpression("\"wind\" + \"HI\""),
        Map(ColumnName("wind") -> ColumnName("Column_0")),
        new SimpleSQLGenerator
      )
    )

    assert(ColumnarSQLExpression("`column_0`") ===
      replaceColumnNames(
        ColumnarSQLExpression("`wind`"),
        Map(ColumnName("wind") -> ColumnName("column_0")),
        new SimpleSQLGenerator(DatabaseType.hive)
      )
    )

    assert("(CASE WHEN (\"column_1\" = 'sunny') THEN 1 ELSE 0 END)" ===
      replaceColumnNames(
        ColumnarSQLExpression("(CASE WHEN (\"outlook\" = 'sunny') THEN 1 ELSE 0 END)"),
        Map(ColumnName("outlook") -> ColumnName("column_1")),
        new SimpleSQLGenerator
      ).sql
    )

    /**
      * Need a fancy parser to pass this test.
      * I was using presto-parser, until I realised it wasn't case sensitive (at least the versions supporting Java 7).
      */
    /*
        assert("(CASE WHEN (\"column_1\" = 'true') THEN '\"wind\"' ELSE 'no \"wind\"' END)" ===
          replaceColumnNames(
            ColumnarSQLExpression("case when \"wind\" = 'true' then '\"wind\"' else 'no \"wind\"' end"),
            Map(ColumnName("wind") -> ColumnName("column_1")),
            new SimpleSQLGenerator
          ).sql
        )
     */
  }

  test("Should not quote function names") {
    assert(ColumnarSQLExpression("EXP(3)") === replaceColumnNames(ColumnarSQLExpression("EXP(3)"), Map(ColumnName("a") -> ColumnName("a1")), new SimpleSQLGenerator))
    assert(ColumnarSQLExpression("POWER(\"a1\", 2)") === replaceColumnNames(ColumnarSQLExpression("POWER(\"a\", 2)"), Map(ColumnName("a") -> ColumnName("a1")), new SimpleSQLGenerator))
  }

}

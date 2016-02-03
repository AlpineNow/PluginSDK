package com.alpine.util

import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnarSQLExpression, LayeredSQLExpressions, ColumnName}
import org.scalatest.FunSuite

/**
  * Created by jenny on 1/7/16.
  */
class SQLUtility$Test extends FunSuite {

  test("Should generate boolean comparisons correctly") {
    assert(""""distA" < "distB" AND "distA" < "distC"""" === SQLUtility.comparedToOthers(ColumnName("distA"), Seq(ColumnName("distB"), ColumnName("distC")), "<", new SimpleSQLGenerator))
  }

  test("Should generate comparison SQL correctly") {
    val comparisonSQL: String = SQLUtility.argMinOrMaxSQL(Map("A" -> ColumnName("distA"), "B" -> ColumnName("distB"), "C" -> ColumnName("distC")).toList, "<", new SimpleSQLGenerator)
    assert("""(CASE WHEN ("distA" < "distB" AND "distA" < "distC") THEN 'A' WHEN ("distB" < "distC") THEN 'B' ELSE 'C' END)""" === comparisonSQL)
  }

  test("Create table from LayeredSQLExpressions") {
    val testExpressions = LayeredSQLExpressions(
      Seq(
        Seq((ColumnarSQLExpression("""EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0)"""), ColumnName("e0"))),
        Seq(("1 + e0", ColumnName("sum")), ("e0", ColumnName("e0"))).map(t => (ColumnarSQLExpression(t._1), t._2)),
        Seq(("1 / sum", ColumnName("baseVal")), ("e0 / sum", ColumnName("ce0"))).map(t => (ColumnarSQLExpression(t._1), t._2)),
        Seq(
          (ColumnarSQLExpression("(CASE WHEN (\"baseVal\" > \"ce0\") THEN 'no' ELSE 'yes' END)"), ColumnName("PRED")),
          (ColumnarSQLExpression("\"baseVal\""), ColumnName("CONF_0")),
          (ColumnarSQLExpression("\"ce0\""), ColumnName("CONF_1"))
        )
      )
    )
    val sql = SQLUtility.createTable(testExpressions, "demo.golfnew", "demo.delete_me", new AliasGenerator, new SimpleSQLGenerator)
    val expectedSQL: String =
      """CREATE TABLE demo.delete_me AS
        | SELECT (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) AS "PRED", "baseVal" AS "CONF_0", "ce0" AS "CONF_1"
        | FROM
        | (SELECT
        | 1 / sum AS "baseVal", e0 / sum AS "ce0"
        | FROM
        | (SELECT
        | 1 + e0 AS "sum", e0 AS "e0"
        | FROM
        | (SELECT
        | EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0) AS "e0"
        | FROM
        | demo.golfnew
        |) AS alias_0
        |) AS alias_1
        |) AS alias_2""".stripMargin.replace("\n", "")
    assert(expectedSQL === sql)
  }

}

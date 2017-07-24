package com.alpine.util

import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import org.scalatest.FunSuite

/**
  * Created by jenny on 1/7/16.
  */
class SQLUtility$Test extends FunSuite {

  import SQLUtility._

  test("Should generate boolean comparisons correctly") {
    assert(""""distA" < "distB" AND "distA" < "distC"""" === comparedToOthers(ColumnName("distA"), Seq(ColumnName("distB"), ColumnName("distC")), "<", new SimpleSQLGenerator))
  }

  test("Should generate null check correctly") {
    val comparisonSQL: String = nullWhenAnyColumnNull(Seq(ColumnName("distA"), ColumnName("distB"),ColumnName("distC")), new SimpleSQLGenerator, "innards")
    assert("""CASE WHEN "distA" IS NULL OR "distB" IS NULL OR "distC" IS NULL THEN NULL ELSE innards END""" === comparisonSQL)
  }

  test("Should generate comparison SQL correctly") {
    val comparisonSQL: String = argMinOrMaxSQL(Map("A" -> ColumnName("distA"), "B" -> ColumnName("distB"), "C" -> ColumnName("distC")).toList, "<", new SimpleSQLGenerator)
    assert(
      """CASE WHEN "distA" IS NULL OR "distB" IS NULL OR "distC" IS NULL THEN NULL ELSE
        | (CASE WHEN ("distA" < "distB" AND "distA" < "distC") THEN 'A' WHEN ("distB" < "distC") THEN 'B' ELSE 'C' END)
        | END""".stripMargin.replace("\n", "") === comparisonSQL)
  }

  test("Should generate min or max SQL correctly for a middle length list") {
    val list = Range(0, 5).map(i => (s"A$i", ColumnName(s"d$i")))
    val comparisonSQL: String = argMinOrMaxSQL(list, "<", new SimpleSQLGenerator)
    val expectedSQL =
      """CASE
        | WHEN "d0" IS NULL OR "d1" IS NULL OR "d2" IS NULL OR "d3" IS NULL OR "d4" IS NULL THEN NULL
        | ELSE (CASE
        | WHEN ("d0" < "d1" AND "d0" < "d2" AND "d0" < "d3" AND "d0" < "d4") THEN 'A0'
        | WHEN ("d1" < "d2" AND "d1" < "d3" AND "d1" < "d4") THEN 'A1'
        | WHEN ("d2" < "d3" AND "d2" < "d4") THEN 'A2'
        | WHEN ("d3" < "d4") THEN 'A3'
        | ELSE 'A4' END) END""".stripMargin.replace("\n", "")
    assert(expectedSQL === comparisonSQL)
  }

  test("Should generate min or max SQL for a long list") {
    val list = Range(0, 2000).map(i => (s"A$i", ColumnName(s"d$i")))
    val comparisonSQL: String = argMinOrMaxSQL(list, "<", new SimpleSQLGenerator)
    //    println(comparisonSQL)
  }

  test("Generate test SQL") {
    // Use this to generate SQL to test performance of argMinOrMaxSQL for different numbers of columns.
    // I find things start to get laggy at around 200.
    val list = Range(0, 500).map(i => (s"A$i", ColumnName(s"d$i")))
    val randomColumns: String = list.map(x => "RANDOM() AS " + x._2.rawName).mkString(", ")
    val comparisonSQL: String = argMinOrMaxSQL(list, "<", new SimpleSQLGenerator)
    //   println(s"SELECT $comparisonSQL FROM (SELECT $randomColumns FROM demo.golfnew) AS hi")
  }

  test("Should generate min or max by row SQL correctly") {
    val list = Range(0, 5).map(i => ColumnName(s"d$i"))
    val comparisonSQL: String = minOrMaxByRowSQL(list, "<", new SimpleSQLGenerator)
    val expectedSQL =
      s"""(CASE
         | WHEN ("d0" < "d1" AND "d0" < "d2" AND "d0" < "d3" AND "d0" < "d4") THEN "d0"
         | WHEN ("d1" < "d2" AND "d1" < "d3" AND "d1" < "d4") THEN "d1"
         | WHEN ("d2" < "d3" AND "d2" < "d4") THEN "d2"
         | WHEN ("d3" < "d4") THEN "d3"
         | ELSE "d4" END)""".stripMargin.replace("\n", "")
    assert(expectedSQL === comparisonSQL)
  }

  test("Should generate min or max by row SQL for a long list") {
    val list = Range(0, 2000).map(i => ColumnName(s"d$i"))
    val comparisonSQL: String = minOrMaxByRowSQL(list, "<", new SimpleSQLGenerator)
    //    println(comparisonSQL)
  }

  test("Should generate group by SQL correctly") {
    val expected = """(CASE WHEN ("outlook" = 'rain') THEN "PRED_1" WHEN ("outlook" = 'sunny') THEN "PRED_2" WHEN ("outlook" = 'overcast') THEN "PRED_3" ELSE NULL END)"""
    val actual = groupBySQL(
      ColumnarSQLExpression("\"outlook\""),
      Map(
        ColumnarSQLExpression("'rain'") -> ColumnarSQLExpression("\"PRED_1\""),
        ColumnarSQLExpression("'sunny'") -> ColumnarSQLExpression("\"PRED_2\""),
        ColumnarSQLExpression("'overcast'") -> ColumnarSQLExpression("\"PRED_3\"")
      )
    )
    assert(expected === actual.sql)
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
    val sql = createTable(testExpressions, "demo.golfnew", "demo.delete_me", new AliasGenerator, new SimpleSQLGenerator)
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

/*
 * COPYRIGHT (C) Jan 25 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.ml

import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ClassificationModelSQLExpressions, ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.{SimpleSQLGenerator, SQLUtility}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/16.
  */
class LogisticRegressionSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator()

  test(" SQL Expression for LOR") {
    val lor = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
      "yes",
      Seq(2.0, 3.0).map(java.lang.Double.valueOf), 4.0)),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
    ).sqlTransformer(simpleSQLGenerator).get
    val sql = lor.getSQL
    val expected = LayeredSQLExpressions(
      Seq(
        Seq((ColumnarSQLExpression("""EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0)"""), ColumnName("e0"))),
        Seq((ColumnarSQLExpression("1 + \"e0\""), ColumnName("sum")), (ColumnarSQLExpression("\"e0\""), ColumnName("e0"))),
        Seq((ColumnarSQLExpression("1 / \"sum\""), ColumnName("baseVal")), (ColumnarSQLExpression("\"e0\" / \"sum\""), ColumnName("ce0"))),
        Seq(
          (ColumnarSQLExpression("(CASE WHEN (\"baseVal\" > \"ce0\") THEN 'no' ELSE 'yes' END)"), ColumnName("PRED"))
        )
      )
    )
    assert(sql === expected)
  }

  test("Confidence SQL for LOR") {
    val lor = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
      "yes",
      Seq(2.0, 3.0).map(java.lang.Double.valueOf), 4.0)),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
    ).sqlTransformer(simpleSQLGenerator).get
    val sql = lor.getClassificationSQL
    val expected = ClassificationModelSQLExpressions(
      labelColumnSQL = ColumnarSQLExpression("(CASE WHEN (\"baseVal\" > \"ce0\") THEN 'no' ELSE 'yes' END)"),
      confidenceSQL = Map("no" -> ColumnarSQLExpression("\"baseVal\""), "yes" -> ColumnarSQLExpression("\"ce0\"")),
      Seq(
        Seq((ColumnarSQLExpression("""EXP(4.0 + "temperature" * 2.0 + "humidity" * 3.0)"""), ColumnName("e0"))),
        Seq((ColumnarSQLExpression("1 + \"e0\""), ColumnName("sum")), (ColumnarSQLExpression("\"e0\""), ColumnName("e0"))),
        Seq((ColumnarSQLExpression("1 / \"sum\""), ColumnName("baseVal")), (ColumnarSQLExpression("\"e0\" / \"sum\""), ColumnName("ce0")))
      )
    )
    assert(sql === expected)
  }

  test("SQL for LOR") {
    val lor = new MultiLogisticRegressionModel(
      Seq(
        SingleLogisticRegression(
          "yes",
          Seq(-0.021, -0.065).map(java.lang.Double.valueOf),
          7.4105
        )
      ),
      "no",
      "play",
      Seq(
        ColumnDef("temperature", ColumnType.Long),
        ColumnDef("humidity", ColumnType.Long)
      )
    ).sqlTransformer(new SimpleSQLGenerator).get
    val sqlExpr = lor.getSQL
    val sql = SQLUtility.createTable(sqlExpr, "demo.golfnew", "demo.delete_me", new AliasGenerator, new SimpleSQLGenerator)
    val expectedSQL =
      """CREATE TABLE demo.delete_me AS
        | SELECT
        | (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) AS "PRED"
        | FROM
        | (SELECT
        | 1 / "sum" AS "baseVal",
        | "e0" / "sum" AS "ce0"
        | FROM
        | (SELECT
        | 1 + "e0" AS "sum",
        | "e0" AS "e0"
        | FROM
        | (SELECT
        | EXP(7.4105 + "temperature" * -0.021 + "humidity" * -0.065) AS "e0"
        | FROM
        | demo.golfnew
        |) AS alias_0
        |) AS alias_1
        |) AS alias_2"""
        .stripMargin.replace("\n", "")
    assert(expectedSQL === sql)
  }

}

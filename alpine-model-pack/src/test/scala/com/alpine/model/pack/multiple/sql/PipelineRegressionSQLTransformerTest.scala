/*
 * COPYRIGHT (C) Jan 28 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.PipelineRowModelTest
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, RegressionModelSQLExpression}
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/28/16.
  */
class PipelineRegressionSQLTransformerTest extends FunSuite {

  test("Should generate prediction SQL correctly") {
    val model = (new PipelineRowModelTest).pipelineRegressionModel
    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val predictionSQL = sqlTransformer.getPredictionSQL
    val expected = RegressionModelSQLExpression(
      ColumnarSQLExpression("""0.0 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0"""),
      List(
        List(
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_0")),
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_1")),
          (ColumnarSQLExpression("""(CASE WHEN ("wind" = 'true') THEN 1 WHEN ("wind" = 'false') THEN 0 ELSE NULL END)"""), ColumnName("wind_0"))
        )
      )
    )
    assert(expected === predictionSQL)
  }

  test("Should generate select SQL correctly") {
    val model = (new PipelineRowModelTest).pipelineRegressionModel
    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val sqlExpressions = sqlTransformer.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sqlExpressions, "demo.golfnew", new AliasGenerator(), new SimpleSQLGenerator)
    val expected =
      """SELECT 0.0 +
        | "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED"
        | FROM (SELECT
        | (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_0",
        | (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_1",
        | (CASE WHEN ("wind" = 'true') THEN 1 WHEN ("wind" = 'false') THEN 0 ELSE NULL END) AS "wind_0"
        | FROM demo.golfnew)
        | AS alias_0
        |""".stripMargin.replace("\n", "")
    assert(expected === selectStatement)
  }

}

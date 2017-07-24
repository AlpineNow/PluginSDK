/*
 * COPYRIGHT (C) Jan 28 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.PipelineRowModelTest
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnarSQLExpression, RegressionModelSQLExpression}
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
      OneHotEncodingModelTest.expectedSQLExpressions.layers
    )
    assert(expected === predictionSQL)
  }

  test("Should generate select SQL correctly") {
    val model = (new PipelineRowModelTest).pipelineRegressionModel
    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val sqlExpressions = sqlTransformer.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sqlExpressions, "demo.golfnew", new AliasGenerator(), new SimpleSQLGenerator)
    // println(selectStatement)
    val expected =
      """SELECT 0.0 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED"
        |FROM (SELECT
        |        (CASE WHEN ("outlook" = 'sunny')
        |          THEN 1
        |         WHEN "outlook" IS NOT NULL
        |           THEN 0
        |         ELSE NULL END) AS "outlook_0",
        |        (CASE WHEN ("outlook" = 'overcast')
        |          THEN 1
        |         WHEN "outlook" IS NOT NULL
        |           THEN 0
        |         ELSE NULL END) AS "outlook_1",
        |        (CASE WHEN ("wind" = 'true')
        |          THEN 1
        |         WHEN "wind" IS NOT NULL
        |           THEN 0
        |         ELSE NULL END) AS "wind_0"
        |      FROM (SELECT
        |              (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain')
        |                THEN "outlook"
        |               ELSE NULL END) AS "outlook",
        |              (CASE WHEN "wind" IN ('true', 'false')
        |                THEN "wind"
        |               ELSE NULL END) AS "wind"
        |            FROM demo.golfnew) AS alias_0) AS alias_1""".stripMargin.replaceAll("\\s+", " ").replace("\n", "")
    assert(expected === selectStatement)
  }

}

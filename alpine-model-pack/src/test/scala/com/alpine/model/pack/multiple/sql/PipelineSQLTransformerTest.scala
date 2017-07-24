/*
 * COPYRIGHT (C) Jan 25 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.pack.multiple.PipelineRowModel
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/16.
  */
class PipelineSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator()

  test("SQL Expression for Pipeline model") {
    val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val linearRegressionModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)

    val pipe = PipelineRowModel(Seq(oneHotModel, linearRegressionModel)).sqlTransformer(simpleSQLGenerator).get
    val sqlExpr = pipe.getSQL
    val expected = LayeredSQLExpressions(
      OneHotEncodingModelTest.expectedSQLExpressions.layers ++
      Seq(
        Seq((ColumnarSQLExpression("0.2 + \"outlook_0\" * 0.9 + \"outlook_1\" * 1.0 + \"wind_0\" * 5.0"), ColumnName("PRED")))
      )
    )
    val createTableSQL = SQLUtility.createTable(sqlExpr, "demo.golfnew", "demo.delete_me", new AliasGenerator, new SimpleSQLGenerator)
    // println(createTableSQL)
    val expectedSQL =
      """
        |CREATE TABLE demo.delete_me AS
        | SELECT
        | 0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED"
        | FROM (SELECT
        | (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_0",
        | (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_1",
        | (CASE WHEN ("wind" = 'true') THEN 1 WHEN "wind" IS NOT NULL THEN 0 ELSE NULL END) AS "wind_0"
        | FROM (SELECT
        | (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END) AS "outlook",
        | (CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END) AS "wind"
        | FROM demo.golfnew) AS alias_0) AS alias_1
        |""".stripMargin.replace("\n", "")
    assert(expected === sqlExpr)
    assert(expectedSQL === createTableSQL)
  }

}

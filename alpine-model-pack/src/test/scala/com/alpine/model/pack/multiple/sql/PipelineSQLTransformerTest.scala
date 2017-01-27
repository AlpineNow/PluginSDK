/*
 * COPYRIGHT (C) Jan 25 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.pack.multiple.PipelineRowModel
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.{SimpleSQLGenerator, SQLUtility}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/16.
  */
class PipelineSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator()

  test("SQL Expression for Pipeline model") {
    val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val linearRegressionModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)

    val pipe = new PipelineRowModel(Seq(oneHotModel, linearRegressionModel)).sqlTransformer(simpleSQLGenerator).get
    val sqlExpr = pipe.getSQL
    val expected = LayeredSQLExpressions(
      Seq(
        List(
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_0")),
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_1")),
          (ColumnarSQLExpression("""(CASE WHEN ("wind" = 'true') THEN 1 WHEN ("wind" = 'false') THEN 0 ELSE NULL END)"""), ColumnName("wind_0"))
        ),
        Seq((ColumnarSQLExpression("0.2 + \"outlook_0\" * 0.9 + \"outlook_1\" * 1.0 + \"wind_0\" * 5.0"), ColumnName("PRED")))
      )
    )
    val createTableSQL = SQLUtility.createTable(sqlExpr, "demo.golfnew", "demo.delete_me", new AliasGenerator, new SimpleSQLGenerator)
    val expectedSQL =
      """
        |CREATE TABLE demo.delete_me AS
        | SELECT
        | 0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED"
        | FROM
        | (SELECT
        | (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_0",
        | (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_1",
        | (CASE WHEN ("wind" = 'true') THEN 1 WHEN ("wind" = 'false') THEN 0 ELSE NULL END) AS "wind_0"
        | FROM demo.golfnew
        |) AS alias_0
        |""".stripMargin.replace("\n", "")
    assert(expected === sqlExpr)
    assert(expectedSQL === createTableSQL)
  }

}

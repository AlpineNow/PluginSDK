/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.ml.{SingleLogisticRegression, MultiLogisticRegressionModel, LinearRegressionModel}
import com.alpine.model.pack.multiple.{ModelWithID, CombinerModel, PipelineRowModel}
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnarSQLExpression, ColumnName, LayeredSQLExpressions}
import com.alpine.util.{SimpleSQLGenerator, SQLUtility}
import org.scalatest.FunSuite

class CombinerSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator

  test("Combining the same model with itself (or something with the same column names) should not produce name collisions.") {

    val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val lirModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)
    val pipeModel = new PipelineRowModel(Seq(oneHotModel, lirModel))

    val combiner = new CombinerModel(Seq(ModelWithID("first", pipeModel), ModelWithID("second", pipeModel)))
    val sqlTransformer = combiner.sqlTransformer(simpleSQLGenerator).get
    val sqlExpressions = sqlTransformer.getSQL
    val createTableSQL = SQLUtility.createTable(sqlExpressions, "demo.golfnew", "demo.delete_me", new AliasGenerator, simpleSQLGenerator)

    val expectedSQL =
      """
        |CREATE TABLE demo.delete_me AS
        | SELECT
        | 0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED",
        | (((0.2 + ("column_0" * 0.9)) + ("column_2" * 1.0)) + ("column_1" * 5.0)) AS "PRED_1"
        | FROM
        | (SELECT
        | (CASE WHEN ("outlook" = 'sunny') THEN 1 ELSE 0 END) AS "outlook_0",
        | (CASE WHEN ("outlook" = 'overcast') THEN 1 ELSE 0 END) AS "outlook_1",
        | (CASE WHEN ("wind" = 'true') THEN 1 ELSE 0 END) AS "wind_0",
        | (CASE WHEN ("outlook" = 'sunny') THEN 1 ELSE 0 END) AS "column_0",
        | (CASE WHEN ("outlook" = 'overcast') THEN 1 ELSE 0 END) AS "column_2",
        | (CASE WHEN ("wind" = 'true') THEN 1 ELSE 0 END) AS "column_1"
        | FROM demo.golfnew
        |) AS alias_0
        |""".stripMargin.replace("\n", "")
    assert(expectedSQL === createTableSQL)
  }

  test("SQL Expression for Logistic Regression combined with carryover columns") {
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
    )
    val unit = UnitModel(Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long)))
    val combinedModel = CombinerModel.make(Seq(unit, lor)).sqlTransformer(simpleSQLGenerator).get
    val sqlExpr = combinedModel.getSQL
    val expected = LayeredSQLExpressions(
      Seq(
        List(
          ("\"temperature\"", ColumnName("column_0")),
          ("\"humidity\"", ColumnName("column_1")),
          ( """EXP(7.4105 + "temperature" * -0.021 + "humidity" * -0.065)""", ColumnName("e0"))
        ).map(t => (ColumnarSQLExpression(t._1), t._2)),
        List(
          ("\"column_0\"", ColumnName("column_0")),
          ("\"column_1\"", ColumnName("column_1")),
          ("1 + \"e0\"", ColumnName("sum")),
          ("\"e0\"", ColumnName("e0"))
        ).map(t => (ColumnarSQLExpression(t._1), t._2)),
        List(
          ("\"column_0\"", ColumnName("column_0")),
          ("\"column_1\"", ColumnName("column_1")),
          ( """1 / "sum"""", ColumnName("baseVal")),
          ("\"e0\" / \"sum\"", ColumnName("ce0"))
        ).map(t => (ColumnarSQLExpression(t._1), t._2)),
        List(
          (ColumnarSQLExpression("\"column_0\""), ColumnName("temperature")),
          (ColumnarSQLExpression("\"column_1\""), ColumnName("humidity")),
          (ColumnarSQLExpression("(CASE WHEN (\"baseVal\" > \"ce0\") THEN 'no' ELSE 'yes' END)"), ColumnName("PRED"))
        )
      )
    )
    assert(expected === sqlExpr)

    val sql = SQLUtility.createTable(sqlExpr, "demo.golfnew", "demo.delete_me", new AliasGenerator, simpleSQLGenerator)
    val expectedSQL =  """CREATE TABLE demo.delete_me AS
                         | SELECT
                         | "column_0" AS "temperature", "column_1" AS "humidity",
                         | (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) AS "PRED"
                         | FROM
                         | (SELECT
                         | "column_0" AS "column_0", "column_1" AS "column_1",
                         | 1 / "sum" AS "baseVal",
                         | "e0" / "sum" AS "ce0"
                         | FROM
                         | (SELECT
                         | "column_0" AS "column_0", "column_1" AS "column_1",
                         | 1 + "e0" AS "sum",
                         | "e0" AS "e0"
                         | FROM
                         | (SELECT
                         | "temperature" AS "column_0", "humidity" AS "column_1",
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

/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.ml.{LinearRegressionModel, MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.model.pack.multiple.{CombinerModel, ModelWithID, PipelineRowModel}
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.apache.commons.lang3.StringUtils
import org.scalatest.FunSuite

class CombinerSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator

  test("Combining the same model with itself (or something with the same column names) should not produce name collisions when using blank ids.") {

    val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val lirModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)
    val pipeModel = PipelineRowModel(Seq(oneHotModel, lirModel))

    val combiner = new CombinerModel(Seq(ModelWithID("", pipeModel), ModelWithID("", pipeModel)))
    val sqlTransformer = combiner.sqlTransformer(simpleSQLGenerator).get
    val sqlExpressions = sqlTransformer.getSQL
    val createTableSQL = SQLUtility.createTable(sqlExpressions, "demo.golfnew", "demo.delete_me", new AliasGenerator, simpleSQLGenerator)

    // println(createTableSQL)

    val expectedSQL =
      """
        |CREATE TABLE demo.delete_me AS
        | SELECT
        |     0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED",
        |     0.2 + "column_2" * 0.9 + "column_4" * 1.0 + "column_3" * 5.0 AS "PRED_1"
        |   FROM (SELECT
        |       (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_0",
        |       (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_1",
        |       (CASE WHEN ("wind" = 'true') THEN 1 WHEN "wind" IS NOT NULL THEN 0 ELSE NULL END) AS "wind_0",
        |       (CASE WHEN ("column_0" = 'sunny') THEN 1 WHEN "column_0" IS NOT NULL THEN 0 ELSE NULL END) AS "column_2",
        |       (CASE WHEN ("column_0" = 'overcast') THEN 1 WHEN "column_0" IS NOT NULL THEN 0 ELSE NULL END) AS "column_4",
        |       (CASE WHEN ("column_1" = 'true') THEN 1 WHEN "column_1" IS NOT NULL THEN 0 ELSE NULL END) AS "column_3"
        |     FROM (SELECT
        |         (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END) AS "outlook",
        |         (CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END) AS "wind",
        |         (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END) AS "column_0",
        |         (CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END) AS "column_1"
        | FROM demo.golfnew) AS alias_0) AS alias_1
        |""".stripMargin

    assert(StringUtils.normalizeSpace(expectedSQL) === createTableSQL)
  }

  test("Model identifiers passed in should take effect in the generated column names.") {

    val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val lirModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)
    val pipeModel = PipelineRowModel(Seq(oneHotModel, lirModel))

    val combiner = new CombinerModel(Seq(ModelWithID("first", pipeModel), ModelWithID("second", pipeModel)))
    val sqlTransformer = combiner.sqlTransformer(simpleSQLGenerator).get
    val sqlExpressions = sqlTransformer.getSQL
    val createTableSQL = SQLUtility.createTable(sqlExpressions, "demo.golfnew", "demo.delete_me", new AliasGenerator, simpleSQLGenerator)

    //println(createTableSQL)

    val expectedSQL =
      """
        |CREATE TABLE demo.delete_me AS
        | SELECT
        |     0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED_first",
        |     0.2 + "column_2" * 0.9 + "column_4" * 1.0 + "column_3" * 5.0 AS "PRED_second"
        |   FROM (SELECT
        |       (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_0",
        |       (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END) AS "outlook_1",
        |       (CASE WHEN ("wind" = 'true') THEN 1 WHEN "wind" IS NOT NULL THEN 0 ELSE NULL END) AS "wind_0",
        |       (CASE WHEN ("column_0" = 'sunny') THEN 1 WHEN "column_0" IS NOT NULL THEN 0 ELSE NULL END) AS "column_2",
        |       (CASE WHEN ("column_0" = 'overcast') THEN 1 WHEN "column_0" IS NOT NULL THEN 0 ELSE NULL END) AS "column_4",
        |       (CASE WHEN ("column_1" = 'true') THEN 1 WHEN "column_1" IS NOT NULL THEN 0 ELSE NULL END) AS "column_3"
        |     FROM (SELECT
        |         (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END) AS "outlook",
        |         (CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END) AS "wind",
        |         (CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END) AS "column_0",
        |         (CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END) AS "column_1"
        | FROM demo.golfnew) AS alias_0) AS alias_1
        |""".stripMargin
    assert(StringUtils.normalizeSpace(expectedSQL) === createTableSQL)
  }

  test("SQL Expression for Logistic Regression combined with carryover columns") {
    val lor = MultiLogisticRegressionModel(
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
          (ColumnarSQLExpression("CASE WHEN \"baseVal\" IS NULL OR \"ce0\" IS NULL THEN NULL ELSE (CASE WHEN (\"baseVal\" > \"ce0\") THEN 'no' ELSE 'yes' END) END"), ColumnName("PRED"))
        )
      )
    )
    assert(expected === sqlExpr)

    val sql = SQLUtility.createTable(sqlExpr, "demo.golfnew", "demo.delete_me", new AliasGenerator, simpleSQLGenerator)
    val expectedSQL =  """CREATE TABLE demo.delete_me AS
                         | SELECT
                         | "column_0" AS "temperature", "column_1" AS "humidity",
                         | CASE WHEN "baseVal" IS NULL OR "ce0" IS NULL THEN NULL ELSE (CASE WHEN ("baseVal" > "ce0") THEN 'no' ELSE 'yes' END) END AS "PRED"
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

  test("Should handle name conflicts when column_* is used as a name") {
    val modelA = UnitModel(Seq(ColumnDef("column_0", ColumnType.String), ColumnDef("column_1", ColumnType.String)))
    val modelB = UnitModel(Seq(ColumnDef("column_0", ColumnType.String), ColumnDef("column_1", ColumnType.String), ColumnDef("column_2", ColumnType.String)))

    val combinerModel = CombinerModel.make(Seq(PipelineRowModel(Seq(modelA, modelA)), PipelineRowModel(Seq(modelB, modelB))))
    val sqlExpressions = combinerModel.sqlTransformer(simpleSQLGenerator).get.getSQL
    sqlExpressions.layers.foreach(layer => {
      val columnNames: Seq[ColumnName] = layer.map(t => t._2)
      assert(columnNames.distinct === columnNames)
    })
  }

}

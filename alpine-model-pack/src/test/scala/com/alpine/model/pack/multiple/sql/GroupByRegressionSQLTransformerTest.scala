package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.pack.multiple.{GroupByRegressionModel, GroupByRegressionModelTest, PipelineRegressionModel}
import com.alpine.model.pack.preprocess.{OneHotEncodedFeature, OneHotEncodingModel, OneHotEncodingModelTest}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.AliasGenerator
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class GroupByRegressionSQLTransformerTest extends FunSuite {

  private val groupByModelTest = new GroupByRegressionModelTest

  val model = groupByModelTest.groupByModel
  val integerModel = groupByModelTest.integerModel
  val doubleModel = groupByModelTest.doubleModel

  private val sqlGenerator: SimpleSQLGenerator = new SimpleSQLGenerator

  test("testGetPredictionSQL") {
    val sqlTransformer = model.sqlTransformer(sqlGenerator).get
    val selectSQL = SQLUtility.getSelectStatement(sqlTransformer.getSQL, "inputTableName", new AliasGenerator(), sqlGenerator)
    val expected = """
                  |SELECT (CASE
                  | WHEN ("z" = 'zee') THEN "PRED"
                  | WHEN ("z" = 'zed') THEN "PRED_1"
                  | ELSE NULL END) AS "PRED"
                  | FROM (SELECT
                  | 0.0 + "d" * 1.0 + "a" * 2.0 + "e" * -1.0 AS "PRED",
                  | 0.0 + "b" * -1.0 + "c" * -2.0 + "e" * 1.0 AS "PRED_1",
                  | "z" AS "z"
                  | FROM inputTableName) AS alias_0
                  |""".stripMargin.replaceAllLiterally("\n", "")
    assert(expected === selectSQL)
  }

  test("testGetPredictionSQL for multiple layers.") {

    val pipe1 = {
      val oneHotEncoderModel = OneHotEncodingModel(Seq(
        OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
        OneHotEncodedFeature(List("true"), "false")
      ),
        Seq(new ColumnDef("outlook", ColumnType.String))
      )
      val oneHotModel = (new OneHotEncodingModelTest).oneHotEncoderModel
      val linearRegressionModel = LinearRegressionModel.make(Seq[Double](0.9, 1, 5), oneHotModel.outputFeatures, 0.2)
      new PipelineRegressionModel(Seq(oneHotEncoderModel), linearRegressionModel)
    }

    val pipe2 = {
      val oneHotEncoderModel = OneHotEncodingModel(Seq(
        OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
        OneHotEncodedFeature(List("true"), "false")
      ),
        Seq(new ColumnDef("outlook", ColumnType.String))
      )
      val linearRegressionModel = LinearRegressionModel.make(Seq[Double](0.5, -1, 3), oneHotEncoderModel.outputFeatures, 0.2)
      new PipelineRegressionModel(Seq(oneHotEncoderModel), linearRegressionModel)
    }

    val groupByModel = new GroupByRegressionModel(new ColumnDef("wind", ColumnType.String), Map("true" -> pipe1, "false" -> pipe2))

    val sqlTransformer = groupByModel.sqlTransformer(sqlGenerator).get
    val selectSQL = SQLUtility.getSelectStatement(sqlTransformer.getSQL, "demo.golfnew", new AliasGenerator(), sqlGenerator)

    val expected = """
                  |SELECT (CASE
                  | WHEN ("wind" = 'true') THEN "PRED"
                  | WHEN ("wind" = 'false') THEN "PRED_1"
                  | ELSE NULL END) AS "PRED"
                  | FROM (SELECT
                  | 0.2 + "outlook_0" * 0.9 + "outlook_1" * 1.0 + "wind_0" * 5.0 AS "PRED",
                  | 0.2 + "column_1" * 0.5 + "column_2" * -1.0 AS "PRED_1",
                  | "column_0" AS "wind" FROM
                  | (SELECT
                  | (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_0",
                  | (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "outlook_1",
                  | (CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "column_1",
                  | (CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END) AS "column_2",
                  | "wind" AS "column_0"
                  | FROM demo.golfnew) AS alias_0) AS alias_1
                  |""".stripMargin.replaceAllLiterally("\n", "")
    assert(expected === selectSQL)
  }

  test("testGetPredictionSQL for integer group by") {
    val sqlTransformer = integerModel.sqlTransformer(sqlGenerator).get
    val selectSQL = SQLUtility.getSelectStatement(sqlTransformer.getSQL, "inputTableName", new AliasGenerator(), sqlGenerator)
    val expected = """
                  |SELECT (CASE
                  | WHEN ("z" = 0) THEN "PRED"
                  | WHEN ("z" = 1) THEN "PRED_1"
                  | ELSE NULL END) AS "PRED"
                  | FROM (SELECT
                  | 0.0 + "d" * 1.0 + "a" * 2.0 + "e" * -1.0 AS "PRED",
                  | 0.0 + "b" * -1.0 + "c" * -2.0 + "e" * 1.0 AS "PRED_1",
                  | "z" AS "z"
                  | FROM inputTableName) AS alias_0
                  |""".stripMargin.replaceAllLiterally("\n", "")
    assert(expected === selectSQL)
  }

  test("testGetPredictionSQL for double group by") {
    val sqlTransformer = doubleModel.sqlTransformer(sqlGenerator).get
    val selectSQL = SQLUtility.getSelectStatement(sqlTransformer.getSQL, "inputTableName", new AliasGenerator(), sqlGenerator)
    val expected = """
                  |SELECT (CASE
                  | WHEN ("z" = 0.0) THEN "PRED"
                  | WHEN ("z" = 1.5) THEN "PRED_1"
                  | ELSE NULL END) AS "PRED"
                  | FROM (SELECT
                  | 0.0 + "d" * 1.0 + "a" * 2.0 + "e" * -1.0 AS "PRED",
                  | 0.0 + "b" * -1.0 + "c" * -2.0 + "e" * 1.0 AS "PRED_1",
                  | "z" AS "z"
                  | FROM inputTableName) AS alias_0
                  |""".stripMargin.replaceAllLiterally("\n", "")
    assert(expected === selectSQL)
  }

}

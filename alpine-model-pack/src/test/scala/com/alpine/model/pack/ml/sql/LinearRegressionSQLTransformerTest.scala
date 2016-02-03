/*
 * COPYRIGHT (C) Jan 25 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.ml.sql

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.pack.multiple.CombinerModel
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.transformer.sql.{RegressionModelSQLExpression, ColumnName, LayeredSQLExpressions, ColumnarSQLExpression}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/16.
  */
class LinearRegressionSQLTransformerTest extends FunSuite {

  private val simpleSQLGenerator = new SimpleSQLGenerator()

  test("SQL Expression for Linear Regression") {
    val lir = new LinearRegressionSQLTransformer(LinearRegressionModel.make(Seq(1.2, 3.4), Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long)), 3), simpleSQLGenerator)
    val sql = lir.getPredictionSQL
    assert(sql === RegressionModelSQLExpression(ColumnarSQLExpression("3.0 + \"temperature\" * 1.2 + \"humidity\" * 3.4")))
  }

  test("SQL Expression for Linear Regression with carryover columns") {
    val lir = LinearRegressionModel.make(Seq(1.2, 3.4), Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long)), 3)
    val unit = UnitModel(Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long)))
    val combinedModel = CombinerModel.make(Seq(unit, lir)).sqlTransformer(simpleSQLGenerator).get
    val sql = combinedModel.getSQL
    val expected = LayeredSQLExpressions(
      Seq(
        Seq(
          (ColumnarSQLExpression("\"temperature\""), ColumnName("temperature")),
          (ColumnarSQLExpression("\"humidity\""), ColumnName("humidity")),
          (ColumnarSQLExpression("3.0 + \"temperature\" * 1.2 + \"humidity\" * 3.4"), ColumnName("PRED"))
        )
      )
    )
    assert(expected === sql)
  }

}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import java.io.ObjectStreamClass

import com.alpine.json.{JsonTestUtil, ModelJsonUtil}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
 * Tests serialization of LinearRegressionModel.
 */
class LinearRegressionModelTest extends FunSuite {

  test("It should serialize correctly") {
    val coefficients = Seq[Double](0.9, 1, 5, -1)
    val inputFeatures = Seq(
      new ColumnDef("x1", ColumnType.Double),
      new ColumnDef("x2", ColumnType.Double),
      new ColumnDef("temperature", ColumnType.Long),
      new ColumnDef("humidity", ColumnType.Double)
    )

    val intercept = 3.4
    val originalModel = LinearRegressionModel.make(coefficients, inputFeatures, intercept)

    JsonTestUtil.testJsonization(originalModel)
  }

  test("Should deserialize Linear Regression with NaN correctly") {
    val json = """{"coefficients":[NaN,NaN],"inputFeatures":[{"columnName":"income","columnType":"Double"},{"columnName":"quantity","columnType":"Double"}],"intercept":0.0,"dependentFeatureName":"age","identifier":""}"""
    val m: LinearRegressionModel = ModelJsonUtil.compactGson.fromJson(json, classOf[LinearRegressionModel])
    val coef = m.coefficients
    coef.foreach(d => {
      assert(d.isNaN)
    })
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[LinearRegressionModel]).getSerialVersionUID === -4888344570084962586L)
  }

}

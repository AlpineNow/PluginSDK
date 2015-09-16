/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.features._
import com.alpine.json.JsonTestUtil
import org.scalatest.FunSuite

/**
 * Tests serialization of LinearRegressionModel.
 */
class LinearRegressionModelTest extends FunSuite {

  test("It should serialize correctly") {
    val coefficients = Seq[Double](0.9, 1, 5, -1)
    val inputFeatures = Seq(
      new FeatureDesc("x1", DoubleType()),
      new FeatureDesc("x2", DoubleType()),
      new FeatureDesc("temperature", LongType()),
      new FeatureDesc("humidity", DoubleType())
    ).map(f => f.asInstanceOf[FeatureDesc[_ <: Number]])

    val intercept = 3.4
    val originalModel = LinearRegressionModel.make(coefficients, inputFeatures, intercept)

    JsonTestUtil.testJsonization(originalModel)
  }

}

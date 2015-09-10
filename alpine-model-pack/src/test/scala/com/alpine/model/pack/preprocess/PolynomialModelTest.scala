/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.{DoubleType, FeatureDesc}
import com.alpine.json.JsonTestUtil
import org.scalatest.FunSuite

/**
 * Tests serialization of PolynomialModel
 * and application of PolynomialTransformer.
 */
class PolynomialModelTest extends FunSuite {

  val exponents = Seq(Seq[Double](1,2,0.0), Seq[Double](0.5,3,2))
  val inputFeatures = {
    Seq(new FeatureDesc("x1", DoubleType()), new FeatureDesc("x2", DoubleType()), new FeatureDesc("x3", DoubleType()))
  }.map(f => f.asInstanceOf[FeatureDesc[_ <: Number]])

  val t = new PolynomialModel(exponents, inputFeatures)

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(t)
  }

  test("Should score correctly") {
    assert(Seq(1d, 1d) === t.transformer.apply(Seq(1,1,1)))
    assert(Seq(1d, 0d) === t.transformer.apply(Seq(1,1,0)))
    assert(Seq(1 * 4d, 1 * 8 * 9d) === t.transformer.apply(Seq(1,2,3)))
    assert(Seq(4 * 1d, 2 * 1 * 2.25) === t.transformer.apply(Seq(4,1,1.5)))
  }

}

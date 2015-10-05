/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import org.scalatest.FunSuite

/**
 * Tests serialization of PolynomialModel
 * and application of PolynomialTransformer.
 */
class PolynomialModelTest extends FunSuite {

  val exponents = Seq(Seq[java.lang.Double](1.0,2.0,0.0), Seq[java.lang.Double](0.5,3.0,2.0))
  val inputFeatures = {
    Seq(new ColumnDef("x1", ColumnType.Double), new ColumnDef("x2", ColumnType.Double), new ColumnDef("x3", ColumnType.Double))
  }

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

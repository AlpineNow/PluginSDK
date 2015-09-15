/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.spark.utils

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import MLlibUtils._


class MLlibUtilsTest extends FunSuite {

  test("testToLabeledPoint") {
    val toLabeledPointFunc = toLabeledPoint(dependentColumnIndex = 2 , independentColumnIndices = Array(1, 3))
    assert(LabeledPoint(2.5, new DenseVector(Array(2.0, 3.0))) === toLabeledPointFunc(Row.fromTuple(null, 2, 2.5, 3)))
    assert(LabeledPoint(2.5, new DenseVector(Array(2.0, 3.0))) === toLabeledPointFunc(Row.fromTuple(3.2, 2L, 2.5F, 3L)))
  }

  test("testAnyToDouble") {
    assert(1.0 === anyToDouble(1L))
    assert(1.0 === anyToDouble(1d))
    assert(1.0 === anyToDouble(1D))
    assert(1.0 === anyToDouble(1F))
    assert(anyToDouble(null).isNaN)
    assert(anyToDouble("Help").isNaN)
  }

}

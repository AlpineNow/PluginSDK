/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.model.pack.util.{TransformerUtil, CastedDoubleSeq}
import com.alpine.transformer.RegressionTransformer

/**
  * Applies a Linear Regression model specified by the coefficients and intercept
  * to a row of numeric data.
  *
  * Note that in the input row is wrapped in CastedDoubleSeq, so the input elements
  * must be castable as java.lang.Number.
  */
class LinearRegressionTransformer(coefficients: Seq[java.lang.Double], intercept: Double = 0) extends RegressionTransformer {

  // Use toArray for indexing efficiency.
  private val coefficientArray = TransformerUtil.javaDoubleSeqToArray(coefficients)

  override def predict(row: Row): Double = {
    val doubleRow = CastedDoubleSeq(row)
    var result = intercept
    var i = 0
    while (i < coefficientArray.length) {
      result += coefficientArray(i) * doubleRow(i)
      i += 1
    }
    result
  }

}



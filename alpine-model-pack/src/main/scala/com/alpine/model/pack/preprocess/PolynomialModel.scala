/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.{DoubleType, FeatureDesc}
import com.alpine.model.RowModel
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.transformer.Transformer

/**
 * If the exponents are a matrix
 * [[1,2,0], [0.5,3,2]]
 * Then the transformation of a row (x1, x2, x3) will be
 * (y1, y2) = (x1 * x2 pow 2, sqrt(x1) * x2 pow 3 * x3 pow 2).
 */
case class PolynomialModel(exponents: Seq[Seq[Double]], inputFeatures: Seq[FeatureDesc[_ <: Number]], override val identifier: String = "") extends RowModel {
  override def transformer: Transformer = PolynomialTransformer(this)

  def outputFeatures: Seq[FeatureDesc[_]] = {
    exponents.indices.map(i => FeatureDesc("y_" + i, DoubleType()))
  }

}

case class PolynomialTransformer(model: PolynomialModel) extends Transformer {

  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)

  private val exponents: Array[Array[Double]] = model.exponents.map(v => v.toArray).toArray

  override def apply(row: Row): Row = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    val result = Array.ofDim[Double](exponents.length)
    var i = 0
    while (i < exponents.length) {
      var x = 1.0
      var j = 0
      while (j < rowAsDoubleArray.length) {
        // math.pow(0,0) = 1 in Scala, so we do not have to check the 0 case.
        x *= math.pow(rowAsDoubleArray(j), exponents(i)(j))
        j += 1
      }
      result(i) = x
      i += 1
    }
    result
  }
}

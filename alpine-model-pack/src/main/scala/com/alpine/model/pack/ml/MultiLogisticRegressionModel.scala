/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.model.ClassificationRowModel
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.ClassificationTransformer

/**
 * @author Jenny Thompson
 *         6/11/15
 */
case class MultiLogisticRegressionModel(singleLORs: Seq[SingleLogisticRegression],
                                      baseValue: String,
                                      dependentFeatureName: String,
                                      inputFeatures: Seq[ColumnDef],
                                      override val identifier: String = "") extends ClassificationRowModel {
  override def transformer = LogisticRegressionTransformer(this)

  @transient lazy val classLabels: List[String] = (singleLORs.map(l => l.dependentValue) ++ List(baseValue)).toList

  override def dependentFeature = new ColumnDef(dependentFeatureName, ColumnType.String)
}

/**
 * Represents a SingleLogisticRegression to be used as one of several in a MultiLogisticRegressionModel.
 *
 * We use java.lang.Double for the type of the numeric values, because the scala Double type information
 * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
 *
 * @param dependentValue The dependent value that the coefficients correspond to.
 * @param coefficients The coefficients for the single Logistic Regression model.
 * @param bias The constant term, that is added to the dot product of the feature coefficient vectors.
 */
case class SingleLogisticRegression(dependentValue: String, coefficients: Seq[java.lang.Double], bias: Double = 0)

case class LogisticRegressionTransformer(model: MultiLogisticRegressionModel) extends ClassificationTransformer {
  
  private val betaArrays = model.singleLORs.map(l => (l.bias, TransformerUtil.javaDoubleSeqToArray(l.coefficients))).toArray
  private val numBetaArrays = betaArrays.length
  
  // Reuse of this means that the scoring method is not thread safe.
  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)
  
  override def scoreConfidences(row: Row): Array[Double] = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    calculateConfidences(rowAsDoubleArray)
  }

  def calculateConfidences(x: Array[Double]): Array[Double] = {
    val p = Array.ofDim[Double](numBetaArrays + 1)
    val rowSize: Int = x.length
    p(numBetaArrays) = 1.0
    var z: Double = 1.0
    var i1: Int = 0
    while (i1 < numBetaArrays) {
      var tmp: Double = betaArrays(i1)._1
      val beta: Array[Double] = betaArrays(i1)._2
      var j: Int = 0
      while (j < rowSize) {
        tmp += x(j) * beta(j)
        j += 1
      }
      p(i1) = Math.exp(tmp)
      z += p(i1)
      i1 += 1
    }
    // Normalize
    var i2: Int = 0
    while (i2 < numBetaArrays + 1) {
      p(i2) /= z
      i2 += 1
    }
    p
  }

  /**
   * The result must always return the labels in the order specified here.
   * @return The class labels in the order that they will be returned by the result.
   */
  override def classLabels: Seq[String] = model.classLabels

}

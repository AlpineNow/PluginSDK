/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.spark.utils

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
 * Helper functions that are useful for working with MLlib.
 */
object MLlibUtils {

  /**
   *
   * @param dependentColumnIndex Index of the dependent column in the column set.
   * @param independentColumnIndices Indices of the independent columns.
   * @return Function mapping a Row of the correct schema to a labelled point.
   */
  def toLabeledPoint(dependentColumnIndex: Int , independentColumnIndices: Seq[Int]): (Row) => LabeledPoint = {
    row =>
      val numColumns: Int = independentColumnIndices.length
      val independentValues: Array[Double] = Array.ofDim(numColumns)
      var i = 0
      while (i < numColumns) {
        independentValues(i) = anyToDouble(row(independentColumnIndices(i)))
        i += 1
      }
      new LabeledPoint(
        anyToDouble(row.get(dependentColumnIndex)),
        new DenseVector(independentValues)
      )
  }

  /**
   * Converts input of type Any to Double.
   * Does this by casting to java.lang.Number,
   * and then taking the double value.
   *
   * Will return Double.NaN in the case of bad input.
   *
   * @param a input to be converted to Double.
   * @return Double representation of the number, or Double.NaN if impossible.
   */
  def anyToDouble(a: Any): Double = {
    try {
      a.asInstanceOf[Number].doubleValue()
    }
    catch {
      case _: NullPointerException => Double.NaN
      case _: ClassCastException => Double.NaN
    }
  }

}

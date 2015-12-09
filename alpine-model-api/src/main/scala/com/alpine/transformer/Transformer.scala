/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.transformer

import com.alpine.result.{ClusteringResult, CategoricalResult, RealResult, MLResult, ClassificationResult}

/**
 * Serialization doesn't have to be maintained between different versions for this object, since it it always created by a model,
 * but it does need to be serializable for use in Spark jobs (for sending to worker nodes).
 *
 */
trait Transformer extends Serializable {

  /**
   * Shorthand for the input / output type of the apply method.
   * Equivalent to Seq[Any].
   */
  type Row = Seq[Any]

  /**
   * This method should be implemented with speed and garbage collection in mind,
   * as it called once per row on potentially huge data-sets.
   *
   * This method is not required to be thread-safe.
   * @param row The row of input to be scored.
   * @return The result from applying the trained model to the row.
   */
  def apply(row: Row): Row

  /**
   * Allows the transformer to specify if the apply method can handle null values
   * in the input row.
   *
   * e.g. a Null value replacement transformer, or a Naive Bayes transformer
   * would naturally handle null values, but a Linear Regression transformer would not.
   *
   * Default value is false.
   * @return Boolean indicating tolerance for null values in input.
   */
  def allowNullValues: Boolean = false

}

trait MLTransformer[A <: MLResult] extends Transformer {
  def score(row: Row): A
}

trait RegressionTransformer extends MLTransformer[RealResult] {
  def apply(row: Row): Row = {
    Seq[Any](predict(row))
  }
  def score(row: Row) = RealResult(predict(row))
  // To bypass boxing, the user can call this method.
  def predict(row: Row): Double
}

trait CategoricalTransformer[A <: CategoricalResult] extends MLTransformer[A] {
  /**
   * The result must always return the labels in the order specified here.
   * @return The class labels in the order that they will be returned by the result.
   */
  def classLabels: Seq[String]

  def apply(row: Row): Seq[Any] = {
    val result = score(row)
    val newRow = Array.ofDim[Any](3)
    newRow(0) = result.value
    newRow(1) = {
      if (result.index > -1) {
        result.details(result.index)
      } else {
        null
      }
    }
    import scala.collection.JavaConverters._
    newRow(2) = (result.labels zip result.details).toMap.asJava
    newRow
  }
}

trait ClusteringTransformer extends CategoricalTransformer[ClusteringResult] {
  def scoreDistances(row: Row): Array[Double]
  def score(row: Row) = ClusteringResult(classLabels, scoreDistances(row))
}

trait ClassificationTransformer extends CategoricalTransformer[ClassificationResult] {
  def scoreConfidences(row: Row): Array[Double]
  def score(row: Row) = ClassificationResult(classLabels, scoreConfidences(row))
}

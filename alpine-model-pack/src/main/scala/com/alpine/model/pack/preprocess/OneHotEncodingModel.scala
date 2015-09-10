/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.{FeatureDesc, IntType}
import com.alpine.model.RowModel
import com.alpine.transformer.Transformer

/**
 * Model to apply one-hot encoding to categorical input features.
 * Result will be a sequence of 1s and 0s.
 */
case class OneHotEncodingModel(oneHotEncodedFeatures: Seq[OneHotEncodedFeature], inputFeatures: Seq[FeatureDesc[_]],  override val identifier: String = "") extends RowModel {

  override def transformer = OneHotEncodingTransformer(oneHotEncodedFeatures)

  def outputFeatures: Seq[FeatureDesc[_]] = {
    inputFeatures.indices.flatMap(i => {
      val p = oneHotEncodedFeatures(i)
      p.hotValues.indices.map(j => new FeatureDesc(inputFeatures(i).name + "_" + j, IntType()))
    })
  }

}

/**
 * One hot encoding for a single feature.
 * The baseValue is encoded as all 0s to ensure a linear independence of the range.
 * @param hotValues values to be encoded as 1 at the corresponding index, 0s elsewhere.
 * @param baseValue value to be encoded as a vector as 0s.
 */
case class OneHotEncodedFeature(hotValues: Seq[String], baseValue: String) {
  def getScorer = SingleOneHotEncoder(this)
}

case class OneHotEncodingTransformer(pivotsWithFeatures: Seq[OneHotEncodedFeature]) extends Transformer {

  // Use toArray for indexing efficiency.
  private val scorers = pivotsWithFeatures.map(x => x.getScorer).toArray

  lazy val outputDim: Int = pivotsWithFeatures.map(t => t.hotValues.size).sum

  override def apply(row: Row): Row = {
    val output = Array.ofDim[Any](outputDim)
    var inputIndex = 0
    var outputIndex = 0
    while (inputIndex < scorers.length) {
      outputIndex = scorers(inputIndex).setFeatures(output, row(inputIndex), outputIndex)
      inputIndex += 1
    }
    output
  }
}

/**
 * Applies One-hot encoding for a single feature.
 * e.g. if
 * hotValues = Seq["apple", "raspberry"]
 * baseValue = "orange"
 *
 * apply("apple") = [1,0]
 * apply("raspberry") = [0,1]
 * apply("orange") = [0,0]
 *
 * apply("banana") throws exception "banana is an unrecognised value".
 *
 * @param transform case class wrapping hot values and base values.
 */
case class SingleOneHotEncoder(transform: OneHotEncodedFeature) {
  @transient lazy val hotValuesArray = transform.hotValues.toArray
  @transient lazy val resultDimension = transform.hotValues.length
  def setFeatures(currentRow: Array[Any], value: Any, startingIndex: Int): Int = {
    if (startingIndex + resultDimension > currentRow.length) {
      throw new Exception("Cannot do this!!")
    } else {
      var found = false
      var i = 0
      while (i < resultDimension) {
        currentRow(startingIndex + i) = if (hotValuesArray(i).equals(value)) {
          found = true
          1
        } else {
          0
        }
        i += 1
      }
      if (!found && !transform.baseValue.equals(value)) {
        // TODO: Error handling.
        throw new Exception(s"""$value is an unrecognised value""")
      }
      startingIndex + i
    }
  }
}

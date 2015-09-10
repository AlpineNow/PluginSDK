/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.FeatureDesc
import com.alpine.model.RowModel
import com.alpine.transformer.Transformer

/**
 * Model that will replace null values in the input row with specified values.
 */
case class NullValueReplacement(replacementValues: Seq[Any], inputFeatures: Seq[FeatureDesc[_]], override val identifier: String = "") extends RowModel {
  override def transformer: Transformer = NullValueReplacer(this)
  override def outputFeatures = inputFeatures
}

case class NullValueReplacer(model: NullValueReplacement) extends Transformer {

  val replacementValues = model.replacementValues.toArray

  override def allowNullValues = true

  override def apply(row: Row): Row = {
    val result = Array.ofDim[Any](row.length)
    var i = 0
    while (i < row.length) {
      val x = row(i)
      result(i) = if (x == null) {
        replacementValues(i)
      } else {
        x
      }
      i += 1
    }
    result
  }
}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack

import com.alpine.features.FeatureDesc
import com.alpine.model.RowModel
import com.alpine.transformer.Transformer

/**
 * Represents a model that carries features through without transforming them.
 * Designed to be used in parallel to other models in the CombinerModel.
 */
case class UnitModel(inputFeatures: Seq[FeatureDesc[_]], override val identifier: String = "") extends RowModel {
  override def transformer: Transformer = UnitTransformer
  override def outputFeatures = inputFeatures
}

/**
 * Applies the unit (a.k.a. identity or no-operation) transformation to the input row.
 * That is, apply returns its input argument.
 */
object UnitTransformer extends Transformer {
  override def apply(row: Row): Row = row
}

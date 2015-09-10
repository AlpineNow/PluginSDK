/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.metadata

import com.alpine.features.FeatureDesc

/**
 * We need this information to figure out the output structure of the Predictor,
 * e.g. if the resultType is ClassificationResult then there are 3 output columns,
 * if it is RealResult then there is one (double type) column.
 * identifier is used as part of the output column names (we may change how this works in the future).
 */
class TransformationSchema(val outputFeatures: Seq[FeatureDesc[_]], val identifier: String = "")

/**
 * If the user provides us with the input feature descriptions, then in the Predictor we will be able to verify that
 * the data set for prediction contains all the necessary columns at design-time (not currently implemented but should be easy to do).
 */
case class DetailedTransformationSchema(inputFeatures: Seq[FeatureDesc[_]],
                                        override val outputFeatures: Seq[FeatureDesc[_]],
                                        override val identifier: String = "")
  extends TransformationSchema(outputFeatures, identifier)

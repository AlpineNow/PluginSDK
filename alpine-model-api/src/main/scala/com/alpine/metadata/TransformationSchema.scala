/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.metadata

import com.alpine.plugin.core.io.ColumnDef

/**
 * We need this information to figure out the output structure of the Predictor,
 * e.g. if the resultType is ClassificationResult then there are 3 output columns,
 * if it is RealResult then there is one (double type) column.
 * identifier is used as part of the output column names (we may change how this works in the future).
 */
class TransformationSchema(val outputFeatures: Seq[ColumnDef], val identifier: String = "")

/**
 * If the user provides us with the input feature descriptions, then in the Predictor we will be able to verify that
 * the data set for prediction contains all the necessary columns at design-time (not currently implemented but should be easy to do).
 */
case class DetailedTransformationSchema(inputFeatures: Seq[ColumnDef],
                                        override val outputFeatures: Seq[ColumnDef],
                                        override val identifier: String = "")
  extends TransformationSchema(outputFeatures, identifier)

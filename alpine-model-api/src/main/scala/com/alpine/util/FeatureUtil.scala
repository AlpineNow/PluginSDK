/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.util

import com.alpine.features.{SparseType, DoubleType, StringType, FeatureDesc}

/**
 * This class is a utility for defining features, in particular the output features of models.
 */
object FeatureUtil {

  private val PRED = "PRED"
  private val CONF = "CONF"
  private val DIST = "DIST"
  private val INFO = "INFO"

  val simpleModelOutputFeatures: Seq[FeatureDesc[_]] = {
    Seq(new FeatureDesc(PRED, StringType()))
  }

  val regressionOutputFeatures: Seq[FeatureDesc[_]] = {
    Seq(new FeatureDesc(PRED, DoubleType()))
  }

  val classificationOutputFeatures: Seq[FeatureDesc[_]] = {
    Seq(
      new FeatureDesc(PRED, StringType()),
      new FeatureDesc(CONF, DoubleType()),
      new FeatureDesc(INFO, SparseType())
    )
  }

  val clusteringOutputFeatures: Seq[FeatureDesc[_]] = {
    Seq(
      new FeatureDesc(PRED, StringType()),
      new FeatureDesc(DIST, DoubleType()),
      new FeatureDesc(INFO, SparseType())
    )
  }

}

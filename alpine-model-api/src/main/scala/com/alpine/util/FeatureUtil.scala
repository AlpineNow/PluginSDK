/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.util

import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
 * This class is a utility for defining features, in particular the output features of models.
 */
object FeatureUtil {

  private val PRED = "PRED"
  private val CONF = "CONF"
  private val DIST = "DIST"
  private val INFO = "INFO"

  val simpleModelOutputFeatures: Seq[ColumnDef] = {
    Seq(new ColumnDef(PRED, ColumnType.String))
  }

  val regressionOutputFeatures: Seq[ColumnDef] = {
    Seq(new ColumnDef(PRED, ColumnType.Double))
  }

  val classificationOutputFeatures: Seq[ColumnDef] = {
    Seq(
      new ColumnDef(PRED, ColumnType.String),
      new ColumnDef(CONF, ColumnType.Double),
      new ColumnDef(INFO, ColumnType.Sparse)
    )
  }

  val clusteringOutputFeatures: Seq[ColumnDef] = {
    Seq(
      new ColumnDef(PRED, ColumnType.String),
      new ColumnDef(DIST, ColumnType.Double),
      new ColumnDef(INFO, ColumnType.Sparse)
    )
  }

}

/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.ml.{ClusterInfo, KMeansModel}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

import scala.collection.immutable.Seq

/**
  * Created by jenny on 5/26/16.
  */
class KMeansPFAConverterTest extends AlpinePFAConversionTest {

  val testModel: KMeansModel = {
    val clusters = Seq(
      ClusterInfo("one", Seq[java.lang.Double](1d, 1d, 1d, 1d, 1d)),
      ClusterInfo("two", Seq[java.lang.Double](2d, 2d, 2d, 2d, 2d)),
      ClusterInfo("three", Seq[java.lang.Double](3d, 3d, 3d, 3d, 3d)),
      ClusterInfo("four", Seq[java.lang.Double](4d, 4d, 4d, 4d, 4d)),
      ClusterInfo("five", Seq[java.lang.Double](5d, 5d, 5d, 5d, 5d))
    )
    val inputFeatures = Seq(
      ColumnDef("a", ColumnType.Double),
      ColumnDef("b", ColumnType.Double),
      ColumnDef("c", ColumnType.Double),
      ColumnDef("d", ColumnType.Double),
      ColumnDef("e", ColumnType.Double)
    )
    KMeansModel(clusters, inputFeatures, "KM")
  }

  val testRows: Seq[Seq[Double]] = {
    Range(0, 10).map(i => testModel.inputFeatures.indices.map(i => math.random * 7))
  }

  test("testToPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

}

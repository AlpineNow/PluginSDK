/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.result.ClusteringResult
import org.scalatest.FunSuite

/**
 * Tests serialization and scoring of KMeansModelTest.
 */
class KMeansModelTest extends FunSuite {

  val clusters = Seq(ClusterInfo("A", Seq(231.5/3, 239.0/3)), ClusterInfo("B", Seq(68.25, 68.75)), ClusterInfo("C", Seq(73.5,92.75)))
  val inputFeatures = Seq(ColumnDef("humidity", ColumnType.Double), ColumnDef("temperature", ColumnType.Double))
  val model = new KMeansModel(clusters, inputFeatures, "KM")

  test("Should serialize correctly.") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should score correctly") {
    val scorer = model.transformer
    val expectedResult = ClusteringResult(Seq("A", "B", "C"), Array(9.476,23.33,13.867))
    assert(expectedResult.equals(scorer.score(Seq(85,85)), 1e-2))
  }

}

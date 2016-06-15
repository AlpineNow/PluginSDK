/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.model.pack.multiple.GroupByClassificationModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.opendatagroup.hadrian.data.{PFAMap, PFARecord}

import scala.collection.JavaConverters._

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class GroupByPFAConverterTest extends AlpinePFAConversionTest {

  val testModel = {
    val modelA = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
      "yes",
      Seq(0.5, -0.5).map(java.lang.Double.valueOf), 1.0)),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
    )

    val modelB = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
      "yes",
      Seq(0.1).map(java.lang.Double.valueOf), -10)),
      "no",
      "play",
      Seq(ColumnDef("temperature", ColumnType.Long))
    )

    new GroupByClassificationModel(ColumnDef("wind", ColumnType.String), Map("true" -> modelA, "false" -> modelB))
  }

  val testRows = {
    val r = scala.util.Random
    Seq(Seq("true", -1, 1), Seq("false", -4, -2)) ++ Range(0, 100).map(i => r.nextBoolean().toString :: testModel.inputFeatures.indices.map(i => r.nextInt(14) - 7).toList)
  }

  test("testToJsonPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

  override def assertResultsEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    //    println(alpineResult + ", " + pfaRecord)
    if (!(alpineResult(1) == 0.5)) { // In this case either prediction is valid.
      assert(alpineResult.head === pfaRecord.get(0), alpineResult + ", " + pfaRecord)
    }
    assertDoublesEqual(alpineResult(1).asInstanceOf[Double], pfaRecord.get(1).asInstanceOf[Double])
    val pfaConfidences: PFAMap[_ <: AnyRef] = pfaRecord.get(2).asInstanceOf[PFAMap[_ <: AnyRef]]
    val alpineConfidences = alpineResult(2).asInstanceOf[java.util.Map[String, Double]].asScala
    assert(alpineConfidences.size === pfaConfidences.size())
    alpineConfidences.keys.foreach(key => {
      assertDoublesEqual(alpineConfidences(key), pfaConfidences.apply(key).asInstanceOf[Double])
    })
  }

}

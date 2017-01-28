/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.opendatagroup.hadrian.data.{PFAMap, PFARecord}
import com.opendatagroup.hadrian.reader.jsonToAst

import scala.collection.JavaConverters._

/**
  * Created by Jennifer Thompson on 5/26/16.
  */
class LogisticRegressionToPFATest extends AlpinePFAConversionTest {

  val testModel = MultiLogisticRegressionModel(Seq(
    SingleLogisticRegression(
      "yes",
      Seq(2.0, -3.0).map(java.lang.Double.valueOf), 4.0
    )),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
  )

  // Want something like:
  private val samplePFA =
    """
      |input: {type: array, items: double}
      |output:
      |  type: record
      |  name: Output
      |  fields:
      |    - {name: prediction, type: boolean}
      |    - {name: confidence, type: double}
      |cells:
      |  model:
      |    type:
      |      type: record
      |      name: Model
      |      fields:
      |        - {name: coeff, type: {type: array, items: double}}
      |        - {name: const, type: double}
      |    init:
      |      coeff: [0.9, 1]
      |      const: 3.4
      |action:
      |  - let:
      |      conf:
      |        m.link.logit:
      |          model.reg.linear:
      |            - input
      |            - cell: model
      |  - let:
      |      prediction: {">": [conf, 0.5]}
      |  - "new": {prediction: prediction, confidence: conf}
      |    type: Output
    """.stripMargin

  private val testRows = {
    val r = scala.util.Random
    Seq(Seq(-1, 1), Seq(-4, -2)) ++ Range(0, 100).map(i => testModel.inputFeatures.indices.map(i => r.nextInt(14) - 7))
  }

  test("testToPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

  test("testToPFA Single LOR") {
    val jsonPFASingle = new LogisticRegressionPFAConverter(testModel).PFAComponentsForSingleLOR("input", None).toJson
    //    println(jsonPFASingle)
    testPFA(jsonToAst(jsonPFASingle), testModel, testRows)
  }

  test("testToPFA Multi LOR") {
    val jsonPFAMulti = new LogisticRegressionPFAConverter(testModel).PFAComponentsForMultiLOR("input", None).toJson
    //    println(jsonPFAMulti)
    testPFA(jsonToAst(jsonPFAMulti), testModel, testRows)
  }

  override def assertResultsEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    //    println(alpineResult + ", " + pfaRecord)
    if (!(alpineResult(1) == 0.5)) {
      // In this case either prediction is valid.
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

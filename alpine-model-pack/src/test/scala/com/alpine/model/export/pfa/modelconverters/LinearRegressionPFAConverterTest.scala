/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.opendatagroup.hadrian.data.PFARecord

/**
  * Created by Jennifer Thompson on 5/26/16.
  */
class LinearRegressionPFAConverterTest extends AlpinePFAConversionTest {

  val testModel: LinearRegressionModel = {
    val coefficients = Seq[Double](0.9, 1, 5, -1)
    val inputFeatures = Seq(
      ColumnDef("x1", ColumnType.Double),
      ColumnDef("x2", ColumnType.Double),
      ColumnDef("temperature", ColumnType.Double),
      ColumnDef("humidity", ColumnType.Double)
    )

    val intercept = 3.4
    LinearRegressionModel.make(coefficients, inputFeatures, intercept)
  }

  // Want something like:
  private val samplePFA =
    """
      |input: {type: array, items: double}
      |output: double
      |cells:
      |  model:
      |    type:
      |      type: record
      |      name: Model
      |      fields:
      |        - {name: coeff, type: {type: array, items: double}}
      |        - {name: const, type: double}
      |    init:
      |      coeff: [0.9, 1, 5, -1]
      |      const: 3.4
      |action:
      |  model.reg.linear:
      |    - input
      |    - cell: model
    """.stripMargin

  private val testRows = {
    Range(0, 100).map(i => testModel.coefficients.indices.map(i => math.random * 7))
  }

  test("testToPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

  override def assertResultsEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    assertRecordOfDoublesEqual(pfaRecord, alpineResult)
  }

}

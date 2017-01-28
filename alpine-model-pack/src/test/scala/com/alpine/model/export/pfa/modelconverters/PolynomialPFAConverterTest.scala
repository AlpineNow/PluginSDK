/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.preprocess.PolynomialModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.opendatagroup.hadrian.data.PFARecord

/**
  * Created by Jennifer Thompson on 6/8/16.
  */
class PolynomialPFAConverterTest extends AlpinePFAConversionTest {

  val exponents = Seq(Seq[java.lang.Double](1.0, 2.0, 0.0), Seq[java.lang.Double](0.5, 3.0, 2.0))
  private val inputFeatures = {
    Seq(ColumnDef("x1", ColumnType.Double), ColumnDef("x2", ColumnType.Double), ColumnDef("x3", ColumnType.Double))
  }

  val testModel = PolynomialModel(exponents, inputFeatures)

  private val testRows = {
    Range(0, 10).map(i => testModel.inputFeatures.indices.map(i => math.random * 7))
  }

  test("testToPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

  override def assertResultsEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    assertRecordOfDoublesEqual(pfaRecord, alpineResult)
  }

}

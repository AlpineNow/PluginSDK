/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.pack.preprocess.{RealFunctionWithIndex, _}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.opendatagroup.hadrian.data.PFARecord

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class RealValuedFunctionsPFAConverterTest extends AlpinePFAConversionTest {

  val inputFeatures = Seq(ColumnDef("x1", ColumnType.Double), ColumnDef("x2", ColumnType.Double))
  val functions = Seq(
    (Exp(), 0), (Exp(), 1), (Log(), 1), (Power(2), 0),
    (Log1p(), 0), (Multiply(3.2), 0), (Divide(4.6), 0), (Add(5.2), 0), (Subtract(5.2), 0), (LinearFunction(4, 5), 0))
  val testModel = RealValuedFunctionsModel(functions.map(t => RealFunctionWithIndex(TypeWrapper(t._1), t._2)), inputFeatures)

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

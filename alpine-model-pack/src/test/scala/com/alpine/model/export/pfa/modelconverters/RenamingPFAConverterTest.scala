/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.preprocess.RenamingModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
  * Created by Jennifer Thompson on 2/1/17.
  */
class RenamingPFAConverterTest extends AlpinePFAConversionTest {

  private val testModel = RenamingModel(
    inputFeatures = Seq(
      ColumnDef("LinearFunction_temperature", ColumnType.Double),
      ColumnDef("LinearFunction_humidity", ColumnType.Double)
    ),
    outputNames = Seq("temperature", "humidity"),
    identifier = "R")

  private val testRows = {
    Range(0, 10).map(_ => testModel.inputFeatures.indices.map(_ => math.random * 7))
  }

  test("testToPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

}

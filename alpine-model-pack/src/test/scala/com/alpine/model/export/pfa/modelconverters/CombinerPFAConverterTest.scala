/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.multiple.CombinerModel
import com.alpine.model.pack.preprocess.{OneHotEncodedFeature, OneHotEncodingModel}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
  * Created by Jennifer Thompson on 6/1/16.
  */
class CombinerPFAConverterTest extends AlpinePFAConversionTest {

  val testModel = {
    val oneHotModel = OneHotEncodingModel(Seq(
      OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
      OneHotEncodedFeature(List("true"), "false")
    ),
      Seq(new ColumnDef("outlook", ColumnType.String), new ColumnDef("wind", ColumnType.String))
    )

    val unitModel = new UnitModel(Seq(
      new ColumnDef("temperature", ColumnType.Long),
      new ColumnDef("humidity", ColumnType.Long)
    ))

    CombinerModel.make(Seq(oneHotModel, unitModel))
  }

  val testRows = Seq(
    Seq("sunny", "true", 83, 95),
    Seq("sunny", "false", 83, 95),
    Seq("rain", "true", 83, 95),
    Seq("rain", "false", 83, 95),
    Seq("overcast", "true", 83, 95),
    Seq("overcast", "false", 83, 95)
  )

  test("testToJsonPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

}

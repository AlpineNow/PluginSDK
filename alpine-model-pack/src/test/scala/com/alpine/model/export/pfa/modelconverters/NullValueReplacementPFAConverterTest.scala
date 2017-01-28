/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.preprocess.NullValueReplacement
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class NullValueReplacementPFAConverterTest extends AlpinePFAConversionTest {

  val testModel = NullValueReplacement(
    Seq[Any](70, "sunny"),
    Seq(
      ColumnDef("humidity", ColumnType.Int),
      ColumnDef("outlook", ColumnType.String)
    )
  )

  val testRows = Seq(
    Seq(null, null),
    Seq(65, null),
    Seq(null, "rainy"),
    Seq(65, "rainy")
  )

  test("testToJsonPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

}

/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.preprocess.{OneHotEncodedFeature, OneHotEncodingModel}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.Transformer
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import scala.util.Try

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
class OneHotEncodingPFAConverterTest extends AlpinePFAConversionTest {

  val testModel = OneHotEncodingModel(Seq(
    OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
    OneHotEncodedFeature(List("true"), "false")
  ),
    Seq(new ColumnDef("outlook", ColumnType.String), new ColumnDef("wind", ColumnType.String))
  )

  val testModelNoBaseValue = OneHotEncodingModel(Seq(
    OneHotEncodedFeature(List("sunny", "overcast", "rain"), None),
    OneHotEncodedFeature(List("true"), "false")
  ),
    Seq(new ColumnDef("outlook", ColumnType.String), new ColumnDef("wind", ColumnType.String))
  )

  val testRows = Seq(
    Seq("sunny", "true"),
    Seq("sunny", "false"),
    Seq("rain", "true"),
    Seq("rain", "false"),
    Seq("overcast", "true"),
    Seq("overcast", "false"),
    Seq("overcast", "maybe"),
    Seq("snow", "false")
  )

  test("testToJsonPFA") {
    fullCorrectnessTest(testModel, testRows)
    fullCorrectnessTest(testModelNoBaseValue, testRows)
  }

  override def testRow(pfaEngine: PFAEngine[AnyRef, AnyRef], alpineTransformer: Transformer, row: Seq[Any]): Unit = {
    val alpineResult: Try[Seq[Any]] = Try(alpineTransformer.apply(row))
    val pfaRecord: AnyRef = prepareEngineInput(pfaEngine, row)
    val pfaResult = Try(pfaEngine.action(pfaRecord))

    if (alpineResult.isFailure) {
      assert(pfaResult.isFailure)
    } else if (pfaResult.isFailure) {
      throw pfaResult.failed.get
    } else {
      assert(pfaResult.isSuccess)
      val castedResult: PFARecord = pfaResult.get.asInstanceOf[PFARecord]
      assertResultsEqual(castedResult, alpineResult.get)
    }
  }

}

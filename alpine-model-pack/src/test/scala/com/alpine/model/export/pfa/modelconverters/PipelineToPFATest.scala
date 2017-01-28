/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.pack.multiple.PipelineRowModel
import com.alpine.model.pack.preprocess.OneHotEncodingModel

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
class PipelineToPFATest extends AlpinePFAConversionTest {

  val testModel: PipelineRowModel = {
    val oneHotEncoderModel: OneHotEncodingModel = new OneHotEncodingPFAConverterTest().testModel

    val liRModel = {
      val coefficients = Seq[Double](0.9, 1, 5)
      val lirInputFeatures = oneHotEncoderModel.transformationSchema.outputFeatures
      LinearRegressionModel.make(coefficients, lirInputFeatures)
    }

    PipelineRowModel(List(oneHotEncoderModel, liRModel))
  }

  val testRows = Seq(
    Seq("sunny", "true"),
    Seq("sunny", "false"),
    Seq("rain", "true"),
    Seq("rain", "false"),
    Seq("overcast", "true"),
    Seq("overcast", "false")
  )

  test("testToJsonPFA") {
    fullCorrectnessTest(testModel, testRows)
  }

}

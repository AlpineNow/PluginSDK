/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.features.FeatureDesc
import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.model.{MLModel, RowModel}
import com.alpine.model.pack.preprocess.{OneHotEncodingModel, OneHotEncodingModelTest}
import org.scalatest.FunSuite

/**
 * Tests serialization of various pipeline models.
 */
class PipelineRowModelTest extends FunSuite {

  val oneHotEncoderModel: OneHotEncodingModel = new OneHotEncodingModelTest().oneHotEncoderModel

  val liRModel = {
    val coefficients = Seq[Double](0.9, 1, 5, -1)
    val lirInputFeatures = oneHotEncoderModel.transformationSchema.outputFeatures.map(f => f.asInstanceOf[FeatureDesc[_ <: Number]])
    LinearRegressionModel(coefficients, lirInputFeatures)
  }

  test("Serialization of the Pipeline Model should work") {
    val pipelineModel = new PipelineRowModel(List[RowModel](oneHotEncoderModel))
    JsonTestUtil.testJsonization(pipelineModel)
  }

  test("Serialization of the Pipeline Regression Model should work") {
    val pipelineModel = new PipelineRegressionModel(List[RowModel](new OneHotEncodingModelTest().oneHotEncoderModel), liRModel)
    JsonTestUtil.testJsonization(pipelineModel)
  }

  test("Should include the classes of each component model, the pipeline model and the MLModel") {
    val classesForLoading = new PipelineRegressionModel(List[RowModel](new OneHotEncodingModelTest().oneHotEncoderModel), liRModel).classesForLoading
    val expectedClasses = Set[Class[_]](classOf[MLModel], classOf[PipelineRegressionModel], classOf[LinearRegressionModel], classOf[OneHotEncodingModel])
    assert(expectedClasses == classesForLoading)
  }

}

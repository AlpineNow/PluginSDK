/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import java.io.ObjectStreamClass

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
    val coefficients = Seq[Double](0.9, 1, 5)
    val lirInputFeatures = oneHotEncoderModel.transformationSchema.outputFeatures
    LinearRegressionModel.make(coefficients, lirInputFeatures)
  }

  test("Serialization of the Pipeline Model should work") {
    val pipelineModel = new PipelineRowModel(List[RowModel](oneHotEncoderModel))
    JsonTestUtil.testJsonization(pipelineModel)
  }

  val pipelineRegressionModel = new PipelineRegressionModel(List[RowModel](new OneHotEncodingModelTest().oneHotEncoderModel), liRModel)
  test("Serialization of the Pipeline Regression Model should work") {
    JsonTestUtil.testJsonization(pipelineRegressionModel)
  }

  test("Should include the classes of each component model, the pipeline model and the MLModel") {
    val classesForLoading = new PipelineRegressionModel(List[RowModel](new OneHotEncodingModelTest().oneHotEncoderModel), liRModel).classesForLoading
    val expectedClasses = Set[Class[_]](classOf[MLModel], classOf[PipelineRegressionModel], classOf[LinearRegressionModel], classOf[OneHotEncodingModel])
    assert(expectedClasses == classesForLoading)
  }

  test("Serial UID should be stable") {
    assertSerialUID(classOf[PipelineRowModel], -5456055908821806710L)
    assertSerialUID(classOf[PipelineRegressionModel], 381487725247733182L)
    assertSerialUID(classOf[PipelineClassificationModel], -2407095602660767904L)
    assertSerialUID(classOf[PipelineClusteringModel], -8221170007141359159L)
  }

  private def assertSerialUID(clazz: Class[_], serialUID: Long) = {
    val l = ObjectStreamClass.lookup(clazz).getSerialVersionUID
    assert(l === serialUID)
  }
}

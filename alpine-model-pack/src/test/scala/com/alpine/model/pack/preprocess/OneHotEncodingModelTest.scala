/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.{FeatureDesc, StringType}
import com.alpine.json.JsonTestUtil
import org.scalatest.FunSuite

/**
 * Tests serialization of OneHotEncodingModel
 * and application of OneHotEncodingTransformer.
 */
class OneHotEncodingModelTest extends FunSuite {

  val oneHotEncoderModel = OneHotEncodingModel(Seq(
    OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
    OneHotEncodedFeature(List("true"), "false")
  ),
    Seq(new FeatureDesc("outlook", StringType()), new FeatureDesc("wind", StringType()))
  )

  test("Serialization of the Pivot transformations should work") {
    val p = OneHotEncodedFeature(List("sunny", "overcast"), "rain")
    JsonTestUtil.testJsonization(p)
  }

  test("Serialization of the OneHotEncoder should work") {
    JsonTestUtil.testJsonization(oneHotEncoderModel)
  }

  test("Should transform input correctly") {
    val t = oneHotEncoderModel.transformer
    assert(Seq[Any](1,0,1) == t.apply(Seq[Any]("sunny","true")))
    assert(Seq[Any](0,1,1) == t.apply(Seq[Any]("overcast","true")))
    assert(Seq[Any](0,0,0) == t.apply(Seq[Any]("rain","false")))
    intercept[Exception] {
      t.apply(Seq[Any]("stormy,true"))
    }
  }

}

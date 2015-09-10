/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.features.{DoubleType, FeatureDesc, LongType}
import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import org.scalatest.FunSuite

/**
 * Tests serialization of CombinerModel.
 */
class CombinerModelTest extends FunSuite {

  test("Should serialize correctly") {
    val oneHotEncoderModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val unit = UnitModel(Seq(new FeatureDesc("temperature", LongType()), new FeatureDesc("humidity", DoubleType())))
    val combinerModel = CombinerModel.make(List(oneHotEncoderModel, unit))
    JsonTestUtil.testJsonization(combinerModel)

    val combinerModelWithIdentifiers = new CombinerModel(Seq(ModelWithID("oneHot", oneHotEncoderModel), ModelWithID("unit", unit)))
    JsonTestUtil.testJsonization(combinerModelWithIdentifiers)
  }

}

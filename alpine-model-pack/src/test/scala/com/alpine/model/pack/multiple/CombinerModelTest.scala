/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import org.scalatest.FunSuite

/**
 * Tests serialization of CombinerModel.
 */
class CombinerModelTest extends FunSuite {

  test("Should serialize correctly") {
    val oneHotEncoderModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val unit = UnitModel(Seq(new ColumnDef("temperature", ColumnType.Long), new ColumnDef("humidity", ColumnType.Double)))
    val combinerModel = CombinerModel.make(List(oneHotEncoderModel, unit))
    JsonTestUtil.testJsonization(combinerModel)

    val combinerModelWithIdentifiers = new CombinerModel(Seq(ModelWithID("oneHot", oneHotEncoderModel), ModelWithID("unit", unit)))
    JsonTestUtil.testJsonization(combinerModelWithIdentifiers)
  }

}

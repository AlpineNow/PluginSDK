/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.preprocess.OneHotEncodingModelTest
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
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

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[CombinerModel]).getSerialVersionUID === -8313917981243536138L)
  }

}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.preprocess.{OneHotEncodingModelTest, PolynomialModel}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
  * Tests serialization of CombinerModel.
  */
class CombinerModelTest extends FunSuite {

  test("Should serialize correctly") {
    val oneHotEncoderModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val unit = UnitModel(Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Double)))
    val combinerModel = CombinerModel.make(List(oneHotEncoderModel, unit))
    JsonTestUtil.testJsonization(combinerModel)

    val combinerModelWithIdentifiers = new CombinerModel(Seq(ModelWithID("oneHot", oneHotEncoderModel), ModelWithID("unit", unit)))
    JsonTestUtil.testJsonization(combinerModelWithIdentifiers)
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[CombinerModel]).getSerialVersionUID === -8313917981243536138L)
  }

  test("Streamlining should work") {
    val oneHotEncoderModel = (new OneHotEncodingModelTest).oneHotEncoderModel
    val unit = UnitModel(Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Double)))
    val combinerModel = CombinerModel.make(List(oneHotEncoderModel, unit))
    val featureNamesToKeep = Seq("temperature", "outlook_0")
    val newModel = combinerModel.streamline(featureNamesToKeep)
    assert(newModel.inputFeatures.map(_.columnName).toSet === Set("temperature", "outlook"))
    assert(featureNamesToKeep.toSet.subsetOf(newModel.outputFeatures.map(_.columnName).toSet))
  }

  test("Streamlining should work with naming collisions") {
    val firstModel = UnitModel(Seq(ColumnDef("y_1", ColumnType.String)))
    val secondModel = PolynomialModel(
      Seq(Seq[java.lang.Double](1.0, 2.0, 0.0), Seq[java.lang.Double](0.5, 3.0, 2.0)),
      Seq(ColumnDef("a", ColumnType.Double), ColumnDef("b", ColumnType.Double), ColumnDef("c", ColumnType.Double))
    )
    val combinerModel = CombinerModel.make(Seq(firstModel, secondModel))
    // Output Features are y_1, y_0_1, y_1_1
    // println(combinerModel.outputFeatures.map(_.columnName))
    val streamlinedModel = combinerModel.streamline(Seq("y_0_1"))
    // This should grab the first output features of the polynomial model.
    // Since that row only as the first two columns as non-zero, it should only need input features 'a' and 'b'.
    assert(streamlinedModel.inputFeatures === Seq(ColumnDef("a", ColumnType.Double), ColumnDef("b", ColumnType.Double)))
    assert(streamlinedModel.outputFeatures === Seq(ColumnDef("y_0_1", ColumnType.Double)))
  }

}

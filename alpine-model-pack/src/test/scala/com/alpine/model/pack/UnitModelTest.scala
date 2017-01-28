/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack

import java.io.ObjectStreamClass

import com.alpine.json.ModelJsonUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/17.
  */
class UnitModelTest extends FunSuite {

  private val oldModelJson =
    """{
      |   "inputFeatures":[
      |      {
      |         "columnName":"outlook",
      |         "columnType":"String"
      |      },
      |      {
      |         "columnName":"humidity",
      |         "columnType":"Long"
      |      },
      |      {
      |         "columnName":"play",
      |         "columnType":"String"
      |      }
      |   ],
      |   "identifier":"U"
      |}""".stripMargin

  private val testModel = UnitModel(
    inputFeatures = Seq(
      ColumnDef("outlook", ColumnType.String),
      ColumnDef("humidity", ColumnType.Long),
      ColumnDef("play", ColumnType.String)
    ),
    identifier = "U")

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[UnitModel]).getSerialVersionUID === -75224626855321481L)
  }

  test("Deserialization of old models should work") {
    val deserializedModel = ModelJsonUtil.compactGson.fromJson(oldModelJson, classOf[UnitModel])
    assert(deserializedModel === testModel)
  }

  test("Streamlining model should work") {
    assert(UnitModel(
      inputFeatures = Seq(
        ColumnDef("humidity", ColumnType.Long),
        ColumnDef("play", ColumnType.String)
      ),
      identifier = "U") === testModel.streamline(Seq("play", "humidity")))
  }

}

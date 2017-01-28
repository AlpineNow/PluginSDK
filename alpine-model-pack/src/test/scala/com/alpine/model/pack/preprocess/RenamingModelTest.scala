/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import com.alpine.json.ModelJsonUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/17.
  */
class RenamingModelTest extends FunSuite {

  private val oldModelJson =
    """{
      |   "inputFeatures":[
      |      {
      |         "columnName":"LinearFunction_temperature",
      |         "columnType":"Double"
      |      },
      |      {
      |         "columnName":"LinearFunction_humidity",
      |         "columnType":"Double"
      |      }
      |   ],
      |   "outputNames":[
      |      "temperature",
      |      "humidity"
      |   ],
      |   "identifier":"R"
      |}""".stripMargin

  private val testModel = RenamingModel(
    inputFeatures = Seq(
      ColumnDef("LinearFunction_temperature", ColumnType.Double),
      ColumnDef("LinearFunction_humidity", ColumnType.Double)
    ),
    outputNames = Seq("temperature", "humidity"),
    identifier = "R")

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[RenamingModel]).getSerialVersionUID === 5576785112360108164L)
  }

  test("Deserialization of old models should work") {
    val deserializedModel = ModelJsonUtil.compactGson.fromJson(oldModelJson, classOf[RenamingModel])
    assert(deserializedModel === testModel)
  }

  test("Streamlining model should work") {
    assert(RenamingModel(
      inputFeatures = Seq(ColumnDef("LinearFunction_humidity", ColumnType.Double)),
      outputNames = Seq("humidity"),
      identifier = "R") === testModel.streamline(Seq("humidity")))
  }

}

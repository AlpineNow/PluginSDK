/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import com.alpine.features._
import com.google.gson.reflect.TypeToken
import org.scalatest.FunSuite
/**
 * @author Jenny Thompson
 *         3/5/15
 */
class GsonTypeAdapterTest extends FunSuite {

  test("Should wrapped types correctly") {
    val originalStringType = StringType()

    val gson = JsonUtil.prettyGson
    val stringTypeJson = gson.toJson(TypeWrapper(StringType()))
 //   println(stringTypeJson)
    val newObj = gson.fromJson(stringTypeJson, classOf[TypeWrapper[DataType[_]]])
    newObj.value match {
      case _: StringType => assert(true)
      case _ => assert(false)
    }
    val newObj2 = gson.fromJson(stringTypeJson, classOf[TypeWrapper[DataType[_]]])
    assert(newObj.value == newObj2.value)
    assert(StringType() == newObj.value)
    assert(originalStringType == newObj2.value)
    assert(originalStringType == newObj.value)
  }

  test("Lists serialize and deserialize properly.") {

    val gson = JsonUtil.compactGson

    val l1 = List("a", "c")
    val stringListType = new TypeToken[List[String]] {}.getType
    val json1 = gson.toJson(l1, stringListType)
  //  println(json1) // ["a","c"]
    val newL1: List[String] = gson.fromJson(json1, stringListType)
    assert(l1 === newL1)

    val l2 = List(1, 3)
    val intListType = new TypeToken[List[Int]] {}.getType
    val json2 = gson.toJson(l2, intListType)
  //  println(json2) // [1,3]
    val newL2: List[Int] = gson.fromJson(json2, intListType)
    assert(l2 === newL2)
  }

  val features = List[FeatureDesc[Any]](
    new FeatureDesc("outlook", StringType()),
    new FeatureDesc("temperature", LongType()),
    new FeatureDesc("humidity", DoubleType()),
    new FeatureDesc("wind",StringType())
  )

  private val featuresObj = new Features(features)

  test("Should serialize known types correctly") {
    val gson = JsonUtil.prettyGsonWithTypeHints
    val stringJson = gson.toJson(featuresObj)
 //   println(stringJson)
    val newFeatures = gson.fromJson(stringJson, classOf[Features[Any]])
    val newFeaturesList: List[FeatureDesc[_]] = newFeatures.features
    assert(newFeaturesList.length === features.length)
    assert(newFeaturesList === features)
    val expectedJson = """{
                         |  "features": [
                         |    {
                         |      "name": "outlook",
                         |      "dataType": {
                         |        "type": "StringType",
                         |        "data": {}
                         |      }
                         |    },
                         |    {
                         |      "name": "temperature",
                         |      "dataType": {
                         |        "type": "LongType",
                         |        "data": {}
                         |      }
                         |    },
                         |    {
                         |      "name": "humidity",
                         |      "dataType": {
                         |        "type": "DoubleType",
                         |        "data": {}
                         |      }
                         |    },
                         |    {
                         |      "name": "wind",
                         |      "dataType": {
                         |        "type": "StringType",
                         |        "data": {}
                         |      }
                         |    }
                         |  ]
                         |}""".stripMargin
    assert(expectedJson === stringJson)
  }

  test("Should serialize unregistered types correctly - by storing class name.") {
    val gson = JsonUtil.prettyGson
    val anyValues: List[TypeWrapper[Any]] = List[Any](1.3, true).map(a => TypeWrapper[Any](a))
    val anyListType = new TypeToken[List[TypeWrapper[Any]]] {}.getType
    val stringJson = gson.toJson(anyValues, anyListType)
    val newValues: List[TypeWrapper[Any]] = gson.fromJson(stringJson, anyListType)
    assert(anyValues === newValues)
 //   println(stringJson)
    val expectedJson = """[
                         |  {
                         |    "type": "java.lang.Double",
                         |    "data": 1.3
                         |  },
                         |  {
                         |    "type": "java.lang.Boolean",
                         |    "data": true
                         |  }
                         |]""".stripMargin
    assert(expectedJson === stringJson)
  }

  test("Known type keys should be unique") {
    val expectedSize = JsonUtil.defaultKnownTypes.typeToKeyMap.size
    assert(JsonUtil.defaultKnownTypes.typeToKeyMap.values.toSet.size === expectedSize)
    assert(JsonUtil.defaultKnownTypes.keyToTypeMap.size === expectedSize)
  }

}

/**
 * Only used for testing serialization.
 */
case class Features[S](features: List[FeatureDesc[_ <: S]])
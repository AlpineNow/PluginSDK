/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import com.alpine.common.serialization.json.TypeWrapper
import com.google.gson.reflect.TypeToken
import org.scalatest.FunSuite
/**
 * @author Jenny Thompson
 *         3/5/15
 */
class GsonTypeAdapterTest extends FunSuite {

  test("Lists serialize and deserialize properly.") {

    val gson = ModelJsonUtil.compactGson

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

  test("Should serialize unregistered types correctly - by storing class name.") {
    val gson = ModelJsonUtil.prettyGson
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

}
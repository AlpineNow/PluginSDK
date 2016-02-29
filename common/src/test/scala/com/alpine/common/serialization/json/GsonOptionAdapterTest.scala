/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.common.serialization.json

import org.scalatest.FunSuite

/**
 * @author Jenny Thompson
 *         8/5/15
 */
class GsonOptionAdapterTest extends FunSuite {

  test("Should serialize None options correctly") {
    JsonTestUtil.testJsonization(None)
  }

  test("Should serialize Some options correctly") {
    JsonTestUtil.testJsonization(Some(1))
  }

  test("Should deserialize unwrapped Some options correctly") {
    val testStringWithSome =
      """{
        |  "planet": "Earth",
        |  "continent": "Antarctica"
        |}""".stripMargin
    JsonTestUtil.testDeserialization(testStringWithSome, Location("Earth", Some("Antarctica")))

    /**
      * Gson deserializes missing fields to null, bypassing the GsonOptionAdapter,
      * so we can't make them automatically default to None.
      */
    val testStringWithNone =
      """{
        |  "planet": "Mars",
        |  "continent": {
        |    "type": "None"
        |  }
        |}""".stripMargin
    JsonTestUtil.testDeserialization(testStringWithNone, Location("Mars", None))

  }

}

case class Location(planet: String, continent: Option[String] = None)
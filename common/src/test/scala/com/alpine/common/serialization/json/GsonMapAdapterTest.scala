/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.common.serialization.json

import org.scalatest.FunSuite

/**
  * @author Jenny Thompson
  *         7/16/15
  */
class GsonMapAdapterTest extends FunSuite {

  test("Should serialize and deserialize map correctly") {
    JsonTestUtil.testJsonization(StringIntMap(Map("a" -> 2, "b" -> 3)))
    JsonTestUtil.testJsonization(Map("a" -> 2, "b" -> 3))
  }

}

case class StringIntMap(map: Map[String, Int])

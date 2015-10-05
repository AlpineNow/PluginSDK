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
class GsonSeqAdapterTest extends FunSuite {

  test("Should serialize and deserialize list correctly") {
    JsonTestUtil.testJsonization(IntList(List(2, 3)))
    JsonTestUtil.testJsonization(List(2, 3))
    JsonTestUtil.testJsonization(StringList(List("a", "b")))
    JsonTestUtil.testJsonization(List("a", "b"))
  }

}

case class IntList(list: List[Int])
case class StringList(list: List[String])

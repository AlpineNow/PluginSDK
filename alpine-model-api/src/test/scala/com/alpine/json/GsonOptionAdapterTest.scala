/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

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

}

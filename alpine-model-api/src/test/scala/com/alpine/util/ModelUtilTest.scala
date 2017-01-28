/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.util

import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/26/17.
  */
class ModelUtilTest extends FunSuite {

  import ModelUtil._

  test("testCumulativeSum") {
    assert(Seq(0) === cumulativeSum(Seq()))
    assert(Seq(0, 2) === cumulativeSum(Seq(2)))
    assert(Seq(0, 0) === cumulativeSum(Seq(0)))
    assert(Seq(0, 2, 3, 5) === cumulativeSum(Seq(2, 1, 2)))
    assert(Seq(0, 1, 3, 6, 10, 15) === cumulativeSum(Range(1,6)))
  }

}

/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */
package com.alpine.model.pack.util

import org.scalatest.FunSuite


class TransformerUtilTest extends FunSuite {

  test("testAnyToDouble") {
    assert(1.0 === TransformerUtil.anyToDouble(1L))
    assert(1.0 === TransformerUtil.anyToDouble(1d))
    assert(1.0 === TransformerUtil.anyToDouble(1D))
    assert(1.0 === TransformerUtil.anyToDouble(1F))
    assert(TransformerUtil.anyToDouble(null).isNaN)
    assert(TransformerUtil.anyToDouble("Help").isNaN)
  }

}

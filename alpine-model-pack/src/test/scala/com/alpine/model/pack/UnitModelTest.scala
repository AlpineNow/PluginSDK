/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack

import java.io.ObjectStreamClass

import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/17.
  */
class UnitModelTest extends FunSuite {

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[UnitModel]).getSerialVersionUID === -75224626855321481L)
  }

}

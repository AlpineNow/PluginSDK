/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/25/17.
  */
class RenamingModelTest extends FunSuite {

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[RenamingModel]).getSerialVersionUID === 5576785112360108164L)
  }

}

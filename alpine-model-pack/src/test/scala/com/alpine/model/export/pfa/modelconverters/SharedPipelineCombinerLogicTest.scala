/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.modelconverters.SharedPipelineCombinerLogic._
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 6/8/16.
  */
class SharedPipelineCombinerLogicTest extends FunSuite {

  test("duplicates") {
    assert(Set[String]() === duplicates(Seq("b", "a")))
    assert(Set("a") === duplicates(Seq("a", "b", "a")))
    assert(Set("a", "b") === duplicates(Seq("a", "b", "a", "c", "b", "a")))
  }

}

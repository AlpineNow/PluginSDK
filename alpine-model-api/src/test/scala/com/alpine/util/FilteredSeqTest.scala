package com.alpine.util

import com.alpine.json.ModelJsonUtil
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class FilteredSeqTest extends FunSuite {

  test("Should filter correctly.") {
    val original = Seq("a", "b", "c", "d")
    val indicesToUse = Seq(3, 0)
    assert(Seq("d", "a") === FilteredSeq(original, indicesToUse))
  }

  test("Only serializes relevant entries of FilteredSeq") {
    val original = Seq("a", "b", "c", "d")
    val indicesToUse = Seq(3, 0)
    val jsonSeq = ModelJsonUtil.compactGson.toJson(FilteredSeq(original, indicesToUse))
    assert("""["d","a"]""" === jsonSeq)
  }

}

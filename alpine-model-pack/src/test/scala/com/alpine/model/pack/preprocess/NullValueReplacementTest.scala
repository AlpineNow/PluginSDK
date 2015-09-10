/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.features.{StringType, FeatureDesc, IntType}
import com.alpine.json.JsonTestUtil
import org.scalatest.FunSuite

/**
 * Tests serialization of NullValueReplacement
 * and application of NullValueReplacer.
 */
class NullValueReplacementTest extends FunSuite {

  val model = NullValueReplacement(
    Seq[Any](70, "sunny"),
    Seq[FeatureDesc[_]](
      FeatureDesc("humidity", IntType()),
      FeatureDesc("outlook", StringType())
    )
  )

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should apply transformation correctly") {
    val t = model.transformer
    assert(Seq(70,"sunny") === t.apply(Seq[Any](null, null)))
    assert(Seq(65,"sunny") === t.apply(Seq[Any](65, null)))
    assert(Seq(70,"rainy") === t.apply(Seq[Any](null, "rainy")))
    assert(Seq(65,"rainy") === t.apply(Seq[Any](65, "rainy")))
  }
}

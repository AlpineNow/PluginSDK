/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import org.scalatest.FunSuite
import org.scalatest.Matchers._


/**
 * Tests scoring of LinearRegressionTransformer.
 */
class LinearRegressionTransformerTest extends FunSuite {

  val coefficients = Seq[java.lang.Double](0.9, 1, 5)

  val intercept = 1d

  val scorer = new LinearRegressionTransformer(coefficients, intercept)

  test("testScore") {
    assert(intercept === scorer.score(Seq[Any](0,0,0)).value)
    assert(intercept + 0.9 === scorer.score(Seq[Any](1,0,0)).value)
    assert(intercept + 0.9 + 1 === scorer.score(Seq[Any](1,1,0)).value)
    assert(intercept + 0.9 + 1 + 5 === scorer.score(Seq[Any](1,1,1)).value)

    for (i <- Range(0,10)) {
      testScorer(Seq[Any](math.random,math.random,math.random))
    }
  }

  def testScorer(input: Seq[Any]): Unit = {
    val result = scorer.score(input)
    val expected = (input.map(x => x.asInstanceOf[Number].doubleValue()) zip coefficients).map(x => x._1 * x._2).sum + intercept
    result.value should equal (expected +- 1E-10)
  }

}

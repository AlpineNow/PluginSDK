/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

/**
 * @author Jenny Thompson
 *         6/10/15
 */
object JsonTestUtil {

  def testJsonization(p: Any, printJson: Boolean = false): Unit = {
    val prettyGson = ModelJsonUtil.prettyGson
    val pJson: String = prettyGson.toJson(p)
    if (printJson) {
      println("Pretty json is:")
      println(pJson)
    }
    val compactGson = ModelJsonUtil.compactGson
    val cJson = compactGson.toJson(p)
    if (printJson) {
      println()
      println("Compact json is:")
      println(cJson)
    }

    assert(p == prettyGson.fromJson(pJson, p.getClass))
    assert(p == compactGson.fromJson(cJson, p.getClass))
    assert(p == prettyGson.fromJson(cJson, p.getClass))
    assert(p == compactGson.fromJson(pJson, p.getClass))
  }

}

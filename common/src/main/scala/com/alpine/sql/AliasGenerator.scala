/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.sql

/**
  * Used to generate a series of unique aliases for use in sub-queries.
  * Behaviour is deterministic, for ease of use in tests.
  *
  * The value of "stem" defaults to "alias".
  * Bear in mind that this should be short to avoid hitting the 30
  * character limit in Oracle.
  */
class AliasGenerator(val stem: String = "alias") {
  var current = -1

  def getNextAlias: String = {
    current += 1
    stem + "_" + current
  }
}

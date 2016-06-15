/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.expressions

/**
  * Will be serialized just as the content of raw, not as {raw: ...}.
  */
trait PFAExpression {
  /**
    * This is the value that will be JSONised.
    */
  def raw: Any
}

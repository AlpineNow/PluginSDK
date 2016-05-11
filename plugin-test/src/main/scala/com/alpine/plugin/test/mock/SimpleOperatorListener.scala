/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.test.mock

import com.alpine.plugin.core.OperatorListener

class SimpleOperatorListener extends OperatorListener {

  override def notifyMessage(msg: String) = {
    println(msg)
  }

  override def notifyError(error: String) = {
    println(error)
  }

}

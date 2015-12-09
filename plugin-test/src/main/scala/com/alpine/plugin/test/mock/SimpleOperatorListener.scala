package com.alpine.plugin.test.mock

import com.alpine.plugin.core.OperatorListener

class SimpleOperatorListener extends OperatorListener {

  override def notifyMessage(msg: String) = {
    println(msg)
  }

  override def notifyError(error: String) = {
    println(error)
  }

  override def notifyProgress(
                               progressBarId: String,
                               currentProgress: Float
                               ) = {
    println(progressBarId + " " + currentProgress)
  }
}

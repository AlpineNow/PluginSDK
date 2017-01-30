/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
  * :: AlpineSdkApi ::
  * A simple string output.
  */
@AlpineSdkApi
trait IOString extends IOBase {
  def value: String
}
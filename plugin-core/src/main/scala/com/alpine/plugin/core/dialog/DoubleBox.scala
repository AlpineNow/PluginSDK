/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.dialog

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait DoubleBox extends DialogElement {
  def getValue: Double
  def getMin: Double
  def getMax: Double
  def getInclusiveMin: Boolean
  def getInclusiveMax: Boolean
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.dialog

import com.alpine.plugin.core.annotation.{Disabled, AlpineSdkApi}

/**
 * :: AlpineSdkApi ::
 * Operator Dialog elements. E.g., individual text boxes, drop-down boxes, etc.
 */
@AlpineSdkApi
trait DialogElement {
  def getId: String
  def getLabel: String
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.dialog

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait Checkboxes extends DialogElement {
  def getValues: Iterator[String]
  def addValue(value: String): Unit
  def setCheckboxSelection(value: String, selected: Boolean): Unit
  def getSelectedValues: Iterator[String]
}

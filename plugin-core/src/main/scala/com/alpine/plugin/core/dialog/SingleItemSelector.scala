/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.dialog

/**
 * This is a trait for multi-choice, single selection dialog elements.
 * E.g., radio buttons, dropdown box, etc.
 */
trait SingleItemSelector extends DialogElement {
  def getValues: Iterator[String]
  def addValue(value: String): Unit
  def getSelectedValue: String
  def setSelectedValue(value: String): Unit
}

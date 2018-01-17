/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.dialog

/**
  * This is a trait for multi-choice, single selection dialog elements.
  * E.g., radio buttons, dropdown box, etc.
  */
trait RowDialog extends DialogElement

trait IRowDialogRow {
  def getRowDialogValues: List[IRowDialogValue]
}

trait IRowDialogValue {
  def getName: String
  def getValue: String
}

case class RowDialogValidation(isValid: Boolean, message: String)

trait RowDialogValidator {
  def validate(values: List[IRowDialogRow]): RowDialogValidation
}

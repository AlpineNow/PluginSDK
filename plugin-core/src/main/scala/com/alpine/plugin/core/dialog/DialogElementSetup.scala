/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.dialog

/**
  * Sealed trait because we do a case match on possible values in OperatorDialog,
  * and we don't allow the Custom operator developer to define new types.
  *
  * Warning: Avoid Option[Int] and Option[Double] as they don't work well in Java
  * (it gets confused about whether they are primitives or doubles).
  *
  * Created by Jennifer Thompson on 3/27/17.
  */
sealed trait DialogElementSetup {

  def id: String

  def label: String

  def isRequired: Boolean

}

case class NumericBound(value: Double, inclusive: Boolean)

case class NumericDialogElementSetup(id: String, label: String, isRequired: Boolean,
    min: Option[NumericBound],
    max: Option[NumericBound],
    defaultValue: Option[String]) extends DialogElementSetup

// defaultValue is represented as a string because workflow variables are allowed.
case class IntegerDialogElementSetup(id: String, label: String, isRequired: Boolean,
    min: Option[NumericBound],
    max: Option[NumericBound],
    defaultValue: Option[String]) extends DialogElementSetup

object IntegerDialogElementSetup {
  def apply(id: String, label: String, isRequired: Boolean,
      min: Option[NumericBound],
      max: Option[NumericBound],
      defaultValue: Int): IntegerDialogElementSetup = {
    IntegerDialogElementSetup(id, label, isRequired, min, max, Option.apply(defaultValue.toString))
  }

  def apply(id: String, label: String, isRequired: Boolean,
      min: Option[NumericBound],
      max: Option[NumericBound]): IntegerDialogElementSetup = {
    IntegerDialogElementSetup(id, label, isRequired, min, max, None)
  }
}

case class ChorusFileSelectorSetup(id: String, label: String, isRequired: Boolean,
    extensionFilter: Set[String], linkText: Option[String]) extends DialogElementSetup

case class StringDialogElementSetup(id: String, label: String, isRequired: Boolean,
    defaultValue: Option[String], regex: Option[String],
    isLarge: Boolean, isPassword: Boolean) extends DialogElementSetup

case class ScriptEditPopupSetup(id: String, label: String, isRequired: Boolean, scriptType: String) extends DialogElementSetup

case class TabularDatasetColumnDropdownSetup(id: String, label: String, isRequired: Boolean, columnFilter: ColumnFilter,
    selectionGroupId: String, parentBoxId: Option[String]) extends DialogElementSetup

case class ParentOperatorSelectorSetup(id: String, label: String, isRequired: Boolean) extends DialogElementSetup

case class DropdownBoxSetup(id: String, label: String, isRequired: Boolean, values: Option[Seq[String]],
    defaultSelection: Option[String]) extends DialogElementSetup

case class RadioButtonSetup(id: String, label: String, isRequired: Boolean, values: Seq[String], defaultSelection: String)
  extends DialogElementSetup


case class RowDialogElement(id: String, label: String, element: DialogElementSetup, percentWidth: Float, minWidth: Int)

case class RowDialogSetup(id: String, label: String, buttonName: String, isRequired: Boolean, elements: Seq[RowDialogElement],
    validator: Option[RowDialogValidator])
  extends DialogElementSetup

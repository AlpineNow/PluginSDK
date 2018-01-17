package com.alpine.plugin.test.mock

import com.alpine.plugin.core.dialog.{IRowDialogRow, IRowDialogValue}

case class IRowDialogRowMock(values: List[IRowDialogValue]) extends IRowDialogRow{
  override def getRowDialogValues: List[IRowDialogValue] = values
}

case class IRowDialogRowValueMock(name: String, value: String) extends IRowDialogValue {
  override def getName: String = name
  override def getValue: String = value
}

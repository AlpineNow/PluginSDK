package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.dialog.ColumnFilter
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TSVAttributes, TabularSchema}
import com.alpine.plugin.test.mock.{DataSourceMock, OperatorDataSourceManagerMock, OperatorParametersMock, OperatorSchemaManagerMock}
import com.alpine.plugin.test.utils.ParameterMockUtil
import org.scalatest.FunSuite

import scala.com.alpine.plugin.test.mock.OperatorDialogMock
import scala.util.Try


class OperatorDialogMockTests extends FunSuite {

  val inputParams: OperatorParametersMock = new OperatorParametersMock("name", "uuid")
  val golfInputSchema = TabularSchema(Seq(
    ColumnDef("outlook", ColumnType.String),
    ColumnDef("temperature", ColumnType.Long),
    ColumnDef("humidity", ColumnType.Long),
    ColumnDef("wind", ColumnType.String),
    ColumnDef("play", ColumnType.String)
  ))

  val dataSourceMock = new DataSourceMock("TestDataSource")
  val schemaManagerMockOneTabular = new OperatorSchemaManagerMock(Some(golfInputSchema))
  val schemaManagerMockNoSchema = new OperatorSchemaManagerMock(None)

  val operatorDataSourceManagerMock = new OperatorDataSourceManagerMock(dataSourceMock)

  val hdfIOInput = HdfsDelimitedTabularDatasetDefault("path", golfInputSchema, TSVAttributes.default, Some(inputParams.operatorInfo()))


  test("Test Column Selectors") {
    //add ColumnSelect

    val id = "id1"
    ParameterMockUtil.addTabularColumn(inputParams, id, "outlook")

    val mockDialog = new OperatorDialogMock(inputParams, hdfIOInput, Some(golfInputSchema))
    mockDialog.addTabularDatasetColumnDropdownBox(id, "Single Column Outlook",
      ColumnFilter.All, "main", required = true)
    val t = Try(mockDialog.addTabularDatasetColumnDropdownBox(id,
      "Single Column Outlook", ColumnFilter.All, "main", required = true))
    assert(t.isFailure, "adding two things with the same name ")

    ParameterMockUtil.addTabularColumn(inputParams, "id2", "outlook")
    val testColumnFilter = Try(mockDialog.addTabularDatasetColumnDropdownBox(
      "id2", "Single Column", ColumnFilter.NumericOnly, "2"))
    assert(testColumnFilter.isFailure, "Test column filter validation")

    val newParams = mockDialog.getNewParameters
    assert(newParams.getTabularDatasetSelectedColumn(id)._2 == "outlook")

  }
}

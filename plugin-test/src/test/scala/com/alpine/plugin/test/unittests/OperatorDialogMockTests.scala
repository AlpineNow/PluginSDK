package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.dialog.{ChorusFile, ColumnFilter}
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TSVAttributes, TabularSchema}
import com.alpine.plugin.test.mock.ChorusAPICallerMock.ChorusFileInWorkspaceMock
import com.alpine.plugin.test.mock._
import com.alpine.plugin.test.utils.OperatorParameterMockUtil
import org.scalatest.FunSuite
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
  val schemaManagerMockOneTabular = new OperatorSchemaManagerMock()
  val schemaManagerMockNoSchema = new OperatorSchemaManagerMock()

  val operatorDataSourceManagerMock = new OperatorDataSourceManagerMock(dataSourceMock)

  val hdfIOInput = HdfsDelimitedTabularDatasetDefault("path", golfInputSchema, TSVAttributes.default)

  test("Test Column Selectors") {

    val id = "id1"
    OperatorParameterMockUtil.addTabularColumn(inputParams, id, "outlook")

    val mockDialog = new OperatorDialogMock(inputParams, hdfIOInput, Some(golfInputSchema))
    mockDialog.addTabularDatasetColumnDropdownBox(id, "Single Column Outlook",
      ColumnFilter.All, "main", required = true)
    val t = Try(mockDialog.addTabularDatasetColumnDropdownBox(id,
      "Single Column Outlook", ColumnFilter.All, "main", required = true))
    assert(t.isFailure, "adding two things with the same name ")

    OperatorParameterMockUtil.addTabularColumn(inputParams, "id2", "outlook")
    val testColumnFilter = Try(mockDialog.addTabularDatasetColumnDropdownBox(
      "id2", "Single Column", ColumnFilter.NumericOnly, "2"))
    assert(testColumnFilter.isFailure, "Test column filter validation")

    val newParams = mockDialog.getNewParameters
    assert(newParams.getTabularDatasetSelectedColumn(id)._2 == "outlook")
  }

  test("test multi value selectors ") {
    val id = "valueSelector"
    OperatorParameterMockUtil.addCheckBoxSelections(inputParams, id, "Red", "Yellow")
    val mockDialog = new OperatorDialogMock(inputParams, hdfIOInput, Some(golfInputSchema))
    mockDialog.addCheckboxes(id, "Checkboxes", Seq("Red", "Yellow", "Blue"), Seq("Blue"))

    val newParams = mockDialog.getNewParameters
    assert(newParams.getStringArrayValue(id).sameElements(Array("Red", "Yellow")))
  }

  test("Chorus workfile selector ") {
    val chorusFile = ChorusFile("workFileName.afm", "workFileId")
    val workfileInWorkspace = Seq(ChorusFileInWorkspaceMock(chorusFile, None, None))
    inputParams.updateChorusAPICaller(new ChorusAPICallerMock(workfileInWorkspace))
    inputParams.setValue("paramId", chorusFile)

    val mockDialog = new OperatorDialogMock(
      overrideParams = inputParams,
      input = hdfIOInput,
      inputSchema = Some(golfInputSchema))

    mockDialog.addChorusFileDropdownBox("paramId", "ChorusFileDropDown", Set(".afm"), isRequired = true)
    val newParams = mockDialog.getNewParameters
    val paramValue = newParams.getChorusFile("paramId")
    assert(paramValue.equals(chorusFile))

    val mockDialogWOMap = new OperatorDialogMock(inputParams.updateChorusAPICaller(ChorusAPICallerMock()), hdfIOInput, None)
    assert(Try(mockDialogWOMap.addChorusFileDropdownBox("paramId", "ChorusFileDropDown",
      Set(".afm"), isRequired = true)).isFailure)
  }

  test("Using string array value and string value to return column selectors") {
    val chorusFile = ChorusFile("workFileName.afm", "workFileId")
    val workfileInWorkspace = Seq(ChorusFileInWorkspaceMock(chorusFile, None, None))
    val p = new OperatorParametersMock("name", "uuid",
      new ChorusAPICallerMock(workfileInWorkspace))
    p.setValue("chorusFile", chorusFile)
    OperatorParameterMockUtil.addTabularColumns(p, "tabularColumns", "outlook", "play")
    OperatorParameterMockUtil.addTabularColumn(p, "tabularColumn", "outlook")

    val mockDialog = new OperatorDialogMock(
      overrideParams = p,
      input = hdfIOInput,
      inputSchema = Some(golfInputSchema))

    mockDialog.addTabularDatasetColumnDropdownBox("tabularColumn", "label", ColumnFilter.All, "a")
    mockDialog.addTabularDatasetColumnCheckboxes("tabularColumns", "label", ColumnFilter.All, "b")
    mockDialog.addChorusFileDropdownBox("chorusFile", "label", Set(".afm"), true)

    val p1 = p.getStringValue("tabularColumn")
    assert(p1.equalsIgnoreCase("outlook"))
    val p2 = p.getStringValue("chorusFile")
    assert(p2.equalsIgnoreCase("workFileId"))
    val p3 = p.getStringArrayValue("tabularColumns")
    assert(p3.contains("outlook") && p3.contains("play"))

  }

}

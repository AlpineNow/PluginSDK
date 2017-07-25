package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.dialog.{ChorusFile, ColumnFilter}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsDelimitedTabularDatasetDefault, IOListDefault, IOStringDefault, Tuple2Default}
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

  val dataSourceMock = DataSourceMock("TestDataSource")
  val schemaManagerMockOneTabular = new OperatorSchemaManagerMock()
  val schemaManagerMockNoSchema = new OperatorSchemaManagerMock()

  val operatorDataSourceManagerMock = new OperatorDataSourceManagerMock(dataSourceMock)

  val hdfIOInput = HdfsDelimitedTabularDatasetDefault("path", golfInputSchema, TSVAttributes.default)

  test("Test Column Selectors") {

    val id = "id1"
    OperatorParameterMockUtil.addTabularColumn(inputParams, id, "outlook")

    val mockDialog = new OperatorDialogMock(inputParams, hdfIOInput)
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
    val mockDialog = new OperatorDialogMock(inputParams, hdfIOInput)
    mockDialog.addCheckboxes(id, "Checkboxes", Seq("Red", "Yellow", "Blue"), Seq("Blue"))

    val newParams = mockDialog.getNewParameters
    assert(newParams.getStringArrayValue(id).sameElements(Array("Red", "Yellow")))
  }

  test("Chorus workfile selector ") {
    val chorusFile = ChorusFile("workFileName.afm", "17")
    inputParams.setChorusFile("paramId", chorusFile)

    val mockDialog = new OperatorDialogMock(
      overrideParams = inputParams,
      input = hdfIOInput)

    mockDialog.addChorusFileDropdownBox("paramId", "ChorusFileDropDown", Set(".afm"), isRequired = true)
    val newParams = mockDialog.getNewParameters
    val paramValue = newParams.getChorusFile("paramId")
    assert(paramValue.get === chorusFile)
  }

  test("Using string array value and string value to return column selectors") {
    val p = new OperatorParametersMock("name", "uuid")
    OperatorParameterMockUtil.addTabularColumns(p, "tabularColumns", "outlook", "play")
    OperatorParameterMockUtil.addTabularColumn(p, "tabularColumn", "outlook")

    val mockDialog = new OperatorDialogMock(
      overrideParams = p,
      input = hdfIOInput)

    mockDialog.addTabularDatasetColumnDropdownBox("tabularColumn", "label", ColumnFilter.All, "a")
    mockDialog.addTabularDatasetColumnCheckboxes("tabularColumns", "label", ColumnFilter.All, "b")

    val p1 = p.getStringValue("tabularColumn")
    assert(p1 === "outlook")
    val p3 = p.getStringArrayValue("tabularColumns")
    assert(p3.contains("outlook") && p3.contains("play"))

  }

  test("Test Multiple Column selectors for io list") {
    val secondInput = HdfsDelimitedTabularDatasetDefault("path",
      TabularSchema(golfInputSchema.getDefinedColumns.map(c => new ColumnDef(c.columnName + "_2",
        c.columnType))), TSVAttributes.default)
    val input = IOListDefault(Seq(hdfIOInput, secondInput),
      Seq(OperatorInfo("1", "golf"),
        OperatorInfo("2", "golf_1")))

    val params = new OperatorParametersMock("3", "output")
    params.setValue("parent1", "1")
    OperatorParameterMockUtil.addTabularColumns(params, "col_select1", "play", "wind")
    params.setValue("parent2", "2")
    OperatorParameterMockUtil.addTabularColumns(params, "col_select2", "play_2", "wind_2")
    val dialog = new OperatorDialogMock(params, input)
    dialog.addParentOperatorDropdownBox("parent1", "Parent 1")
    dialog.addParentOperatorDropdownBox("parent2", "Parent 2")
    dialog.addTabularDatasetColumnCheckboxes("col_select1", "Column Selector 1", ColumnFilter.All, "a", true, Some("parent1"))
    dialog.addTabularDatasetColumnCheckboxes("col_select2", "Column Selector 2", ColumnFilter.All, "b", true, Some("parent2"))
    assert(dialog.getNewParameters.contains("col_select2"))
  }

  test("Test Single Column selectors for io list") {
    val secondInput = HdfsDelimitedTabularDatasetDefault("path",
      TabularSchema(golfInputSchema.getDefinedColumns.map(c => new ColumnDef(c.columnName + "_2",
        c.columnType))), TSVAttributes.default)
    val input = IOListDefault(Seq(hdfIOInput, secondInput),
      Seq(OperatorInfo("1", "golf"),
        OperatorInfo("2", "golf_1")))

    val params = new OperatorParametersMock("3", "output")
    params.setValue("parent1", "1")
    OperatorParameterMockUtil.addTabularColumn(params, "col_select1", "play")
    params.setValue("parent2", "2")
    OperatorParameterMockUtil.addTabularColumn(params, "col_select2", "play_2")
    val dialog = new OperatorDialogMock(params, input)
    dialog.addParentOperatorDropdownBox("parent1", "Parent 1")
    dialog.addParentOperatorDropdownBox("parent2", "Parent 2")
    dialog.addTabularDatasetColumnDropdownBox("col_select1", "Column Selector 1", ColumnFilter.All, "a", true, Some("parent1"))
    dialog.addTabularDatasetColumnDropdownBox("col_select2", "Column Selector 2", ColumnFilter.All, "b", true, Some("parent2"))
    assert(dialog.getNewParameters.contains("col_select2"))
  }

  test("Test schema method for tuple  "){

    val tupleInput = Tuple2Default(hdfIOInput, IOStringDefault("I am just a string"), hdfIOInput.addendum)
    val p = new OperatorParametersMock("name", "uuid")
    OperatorParameterMockUtil.addTabularColumns(p, "tabularColumns", "outlook", "play")
    OperatorParameterMockUtil.addTabularColumn(p, "tabularColumn", "outlook")

    val mockDialog = new OperatorDialogMock(
      overrideParams = p,
      input = tupleInput )

    mockDialog.addTabularDatasetColumnDropdownBox("tabularColumn", "label", ColumnFilter.All, "a")
    mockDialog.addTabularDatasetColumnCheckboxes("tabularColumns", "label", ColumnFilter.All, "b")

    val p1 = p.getStringValue("tabularColumn")
    assert(p1 === "outlook")
    val p3 = p.getStringArrayValue("tabularColumns")
    assert(p3.contains("outlook") && p3.contains("play"))
  }

}

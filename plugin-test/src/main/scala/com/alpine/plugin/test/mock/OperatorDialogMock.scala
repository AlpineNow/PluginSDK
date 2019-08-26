package com.alpine.plugin.test.mock

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog._
import com.alpine.plugin.core.io.ColumnType.TypeValue
import com.alpine.plugin.core.io._
import com.alpine.plugin.test.utils.OperatorParameterMockUtil

import scala.collection.JavaConversions._
import scala.util.{Success, Try}

abstract class DefaultDialogElementMock(val getId: String,
  val getLabel: String,
  val isRequired: Boolean = true) extends DialogElement {}

abstract class SingleElementSelectorMock(availableValues: Seq[String],
  val getSelectedValue: String,
  val getId: String,
  val getLabel: String,
  val isRequired: Boolean = true) extends SingleItemSelector {

  assert(availableValues.distinct.length.equals(availableValues.length),
    "The available values for the parameters " + getLabel + "are not distinct")
  assert(availableValues.contains(getSelectedValue), "The value " + getSelectedValue +
    " is not one of the available values for the parameter " + getLabel)
}

abstract class AbstractCheckboxMock(availableValues: Seq[String],
  selected: Seq[String],
  val getId: String,
  val getLabel: String,
  val isRequired: Boolean = true) extends Checkboxes {

  //validation logic
  selected.foreach(v =>
    assert(availableValues.contains(v), "The value " + v +
      " is not one of the available values for the parameter " + getLabel))
  assert(availableValues.distinct.length == availableValues.length,
    "The available values for the parameters " + getLabel + "are not distinct")
}

class OperatorDialogMock(overrideParams: OperatorParametersMock,
  input: IOBase) extends OperatorDialog {

  private val dialogElements = scala.collection.mutable.Map[String, DialogElement]()
  private val selectionGroupIdMap = scala.collection.mutable.Map[String, Set[String]]()

  val schemas: Seq[(String, TabularSchema)] = getSchemas(input)

  private val operatorParametersMock = new OperatorParametersMock(
    overrideParams.operatorInfo().name,
    overrideParams.operatorInfo().uuid
  )

  private val uuids: Seq[String] = {
    input match {
      case i: IOList[IOBase] => i.sources.map(_.uuid)
      case _ => Seq[String]()
    }
  }

  private def getSchemas(input: IOBase): Seq[(String, TabularSchema)] = {
    input match {
        //This doesn't actually address  the case of nested lists, since in that case we are perhaps
      // overriding the UUId of the nested list with the "parent" list operator UUID however it
      // will handle the case when we have a list of object  that do not have tabular schemas
      case i: IOList[IOBase] => i.sources.zip(i.elements).flatMap{ case (o, e) => getSchemas(e).map(x => (o.uuid, x._2)) }
      case t: com.alpine.plugin.core.io.Tuple => t.elements.flatMap(getSchemas(_))
      case table: TabularDataset => Seq(("", table.tabularSchema))
      case _ => Seq[(String, TabularSchema)]()
    }
  }

  def getTypeMapForColumnSelector(parentBoxID: Option[String],
    id: String): Map[String, TypeValue] = {

    assert(schemas.nonEmpty, "Cannot create tabular dataset column dropdown " + id + " for " +
      "non tabular input")

    val tabularSchema = if (parentBoxID.nonEmpty) {
      val de = dialogElements.get(parentBoxID.get)
      assert(de.isDefined && de.get.isInstanceOf[ParentOperatorDropdownBox],
        "Column selector " + id + " " +
          "contains parent id " + parentBoxID + "" +
          " but no parent box with this id has been added to this operator dialog.")
      val selectedValue = de.get.asInstanceOf[SingleElementSelectorMock].getSelectedValue
      schemas.filter(selectedValue == _._1).head._2

    } else {
      schemas.head._2
    }

    val nameTypeMap: Map[String, TypeValue] = tabularSchema.getDefinedColumns.map(x =>
      (x.columnName, x.columnType)).toMap
    nameTypeMap
  }

  private def updateDialogElements[T <: DialogElement](id: String, de: T): T = {
    assert(!dialogElements.contains(id), " You have already added a dialog element with key " + id)
    dialogElements.update(id, de)
    de
  }

  private def setStringValue(id: String, defaultSelection: String): String = {
    val selection = Try(overrideParams.getStringValue(id)) match {
      case Success(s) => s
      case _ => defaultSelection
    }

    operatorParametersMock.setValue(id, selection)
    selection
  }

  private def validateColumnSelectionAndUpdateSelectionGroup(column: String,
    selectionGroupId: String, filter: ColumnFilter, nameTypeMap: Map[String, TypeValue]): String = {

    val inputColumns = nameTypeMap.keySet

    assert(inputColumns.contains(column), "Column: " + column + " is not present in input schema.")

    val group = selectionGroupIdMap.getOrElseUpdate(selectionGroupId, Set())
    assert(!group.contains(column), "The selection group id: " + selectionGroupId +
      " already contains the column " + column)
    assert(column.matches(filter.acceptedNameRegex))
    val t = nameTypeMap(column)
    assert(filter.accepts(ColumnDef(column, t)), "The column, " + column + " has type " + t.name +
      " which is not one of the accepted type for the column filter which accepts: ")
    selectionGroupIdMap.update(selectionGroupId, group.union(Set(column)))
    column
  }

  /**
    * Returns a new mock parameters object with all the parameters that were added to this dialog object.
    */
  def getNewParameters: OperatorParametersMock = operatorParametersMock

  override def getLabel: String = "Label"

  def addStringBox(setup: StringDialogElementSetup): StringBox = {
    val selected = setStringValue(setup.id, setup.defaultValue.get)

    class StringBoxImpl extends DefaultDialogElementMock(setup.id, setup.label, setup.isRequired) with StringBox {
      assert(selected.matches(setup.regex.get), "The string: " + selected + "does not conform to regex: " + setup.regex.get)
    }

    val de = new StringBoxImpl()
    updateDialogElements(setup.id, de)
  }

  override def addStringBox(id: String, label: String, defaultValue: String, regex: String, width: Int, height: Int): StringBox = {
    addStringBox(new StringDialogElementSetup(id, label, isRequired = true, Option.apply(defaultValue), Option.apply(regex),
      isLarge = false, isPassword = false))
  }

  override def addStringBox(id: String, label: String, defaultValue: String, regex: String, required: Boolean): StringBox = {
    addStringBox(new StringDialogElementSetup(id, label, required, Option.apply(defaultValue), Option.apply(regex),
      isLarge = false, isPassword = false))
  }

  override def addLargeStringBox(id: String, label: String, defaultValue: String, regex: String, required: Boolean): StringBox = {
    addStringBox(new StringDialogElementSetup(id, label, required, Option.apply(defaultValue), Option.apply(regex),
      isLarge = true, isPassword = false))
  }

  override def addPasswordBox(id: String, label: String, regex: String, required: Boolean): StringBox = {
    addStringBox(new StringDialogElementSetup(id, label, required, Option.apply(""), Option.apply(regex),
      isLarge = false, isPassword = true))
  }

  override def getDialogElement(id: String): DialogElement = dialogElements(id)

  override def getDialogElements: Seq[DialogElement] = dialogElements.values.toSeq

  override def addDialogElement(des: DialogElementSetup): DialogElement = {
    des match {
      case n: NumericDialogElementSetup => {
        addDoubleBox(n)
      }
      case i: IntegerDialogElementSetup => {
        addIntegerBox(i)
      }
      case c: ChorusFileSelectorSetup => {
        addChorusFileDropdownBox(c)
      }
      case s: StringDialogElementSetup => {
        addStringBox(s)
      }
      case s: ScriptEditPopupSetup => {
        addScriptEditPopup(s)
      }
      case p: ParentOperatorSelectorSetup => {
        addParentOperatorDropdownBox(p)
      }
      case t: TabularDatasetColumnDropdownSetup => {
        addTabularDatasetColumnDropdownBox(t)
      }
      case d: DropdownBoxSetup => {
        addDropdownBox(d)
      }
      case r: RadioButtonSetup => {
        addRadioButtons(r)
      }
      case r: RowDialogSetup => {
        addRowDialog(r)
      }
    }
  }

  override def addCheckboxes(id: String, label: String, values: Seq[String], defaultSelections: Seq[String], required: Boolean): Checkboxes = {
    val selections: Seq[String] = Try(overrideParams.getStringArrayValue(id)) match {
      case Success(s) => s
      case _ => defaultSelections
    }

    OperatorParameterMockUtil.addCheckBoxSelections(operatorParametersMock, id, selections: _ *)

    class CheckBoxImpl extends AbstractCheckboxMock(values,
      selections, id, label, required) {}

    val de = new CheckBoxImpl
    updateDialogElements(id, de)
  }

  override def addCheckboxesFromJavaList(id: String, label: String, values: java.util.List[String], defaultSelections: java.util.List[String], required: Boolean): Checkboxes = {
    addCheckboxesFromJavaList(id, label, asScalaBuffer(values).toList, asScalaBuffer(defaultSelections).toList, required)
  }

  def addDropdownBox(setup: DropdownBoxSetup): DropdownBox = {
    val s = setStringValue(setup.id, setup.defaultSelection.get)
    class DropDownBoxImpl extends SingleElementSelectorMock(setup.values.get, s, setup.id, setup.label) with
      DropdownBox {
    }
    //toDo: ditch this sequence, do validation at object creation --> less mutable
    val de = new DropDownBoxImpl
    updateDialogElements(setup.id, de)
  }

  override def addDropdownBox(id: String, label: String, values: Seq[String], defaultSelection: String): DropdownBox = {
    addDropdownBox(new DropdownBoxSetup(id, label, isRequired = true, Option.apply(values), Option.apply(defaultSelection)))
  }

  override def addDropdownBoxFromJavaList(id: String, label: String, values: java.util.List[String], defaultSelection: String): DropdownBox = {
    addDropdownBox(new DropdownBoxSetup(id, label, isRequired = true, Option.apply(asScalaBuffer(values).toList), Option.apply(defaultSelection)))
  }

  override def addHdfsDirectorySelector(id: String, label: String, defaultPath: String): HdfsFileSelector = {
    addHdfsDirectorySelector(id, label, defaultPath, required = true)
  }

  override def addHdfsDirectorySelector(id: String, label: String, defaultPath: String, required: Boolean): HdfsFileSelector = {
    setStringValue(id, defaultPath)

    class HdfsFileSelectorImp extends DefaultDialogElementMock(id, label, required) with HdfsFileSelector

    val de = new HdfsFileSelectorImp
    updateDialogElements(id, de)
  }


  override def addOutputDirectorySelector(id: String, label: String, required: Boolean): HdfsFileSelector = {
    //TODO: Genericize this so that the "tds" part is not hard-coded?
    setStringValue(id, "@default_tempdir/tds_out/@user_name/@flow_name")
    class HdfsFileSelectorImp extends DefaultDialogElementMock(id, label, required) with HdfsFileSelector
    val de = new HdfsFileSelectorImp
    updateDialogElements(id, de)
  }

  override def addDataSourceDropdownBox(id: String, label: String,
    dataSourceManager: OperatorDataSourceManager): DataSourceDropdownBox = {
    setStringValue(id, dataSourceManager.getRuntimeDataSource.getName)
    val allSources = dataSourceManager.getAvailableDataSources.map(_.getName)
    class DataSourceDropdownBoxImpl extends DefaultDialogElementMock(id, label, true) with DataSourceDropdownBox

    val de = new DataSourceDropdownBoxImpl
    updateDialogElements(id, de)
  }


  override def addTabularDatasetColumnDropdownBox(id: String,
                                                  label: String,
                                                  columnFilter: ColumnFilter,
                                                  selectionGroupId: String,
                                                  required: Boolean): TabularDatasetColumnDropdownBox = {
    addTabularDatasetColumnDropdownBox(new TabularDatasetColumnDropdownSetup(id, label, required, columnFilter,
      selectionGroupId, None))
  }


  override def addTabularDatasetColumnDropdownBox(id: String,
                                                  label: String,
                                                  columnFilter: ColumnFilter,
                                                  selectionGroupId: String,
                                                  required: Boolean,
                                                  parentBoxID: Option[String]): TabularDatasetColumnDropdownBox = {
    addTabularDatasetColumnDropdownBox(new TabularDatasetColumnDropdownSetup(id, label, required, columnFilter,
      selectionGroupId, parentBoxID))
  }

  def addTabularDatasetColumnDropdownBox(setup: TabularDatasetColumnDropdownSetup) = {
    val columnTypeMap = getTypeMapForColumnSelector(setup.parentBoxId, setup.id)

    val selection: String = Try(overrideParams.getTabularDatasetSelectedColumn(setup.id)) match {
      case Success(s) =>
        val column = s._2
        validateColumnSelectionAndUpdateSelectionGroup(column, setup.selectionGroupId, setup.columnFilter, columnTypeMap)
      case _ => ""
    }

    assert(!(setup.isRequired && selection.equals("")))

    OperatorParameterMockUtil.addTabularColumn(operatorParametersMock, setup.id, selection)


    class TabularDatasetColumnDropdownBoxImpl extends
      SingleElementSelectorMock(
        columnTypeMap.keys.toSeq, selection, setup.id, setup.label, isRequired = setup.isRequired)
      with TabularDatasetColumnDropdownBox {

      assert(columnTypeMap.keySet.contains(selection),
        "The input of this operator does not contain the column " + selection +
          "in the input schema.")
    }

    val de = new TabularDatasetColumnDropdownBoxImpl
    updateDialogElements(setup.id, de)
  }

  override def addDBSchemaDropdownBox(id: String, label: String, defaultSchema: String): DBSchemaDropdownBox = {
    val selected = setStringValue(id, defaultSchema)
    class DBSchemaDropdownBoxImpl extends SingleElementSelectorMock(Seq(selected), selected, id, label)
      with DBSchemaDropdownBox {}

    val de = new DBSchemaDropdownBoxImpl
    updateDialogElements(id, de)

  }

  def addRadioButtons(setup: RadioButtonSetup): RadioButtons = {
    val selected = setStringValue(setup.id, setup.defaultSelection)

    class RadioButtonImpl extends SingleElementSelectorMock(setup.values, selected, setup.id, setup.label) with RadioButtons

    val de = new RadioButtonImpl
    updateDialogElements(setup.id, de)
  }

  override def addRadioButtons(id: String, label: String, values: Seq[String], defaultSelection: String): RadioButtons = {
    addRadioButtons(new RadioButtonSetup(id, label, isRequired = true, values, defaultSelection))
  }

  override def addRadioButtonsFromJavaList(id: String, label: String, values: java.util.List[String], defaultSelection: String): RadioButtons = {
    addRadioButtons(new RadioButtonSetup(id, label, isRequired = true, asScalaBuffer(values).toList, defaultSelection))
  }

  def addIntegerBox(setup: IntegerDialogElementSetup): IntegerBox = {
    val selection = Try(overrideParams.getIntValue(setup.id)) match {
      case Success(s) => s
      case _ => setup.defaultValue.get.toInt
    }
    require(selection >= setup.min.get.value || selection <= setup.max.get.value, "Selection:" + selection + " is not in range for integer parameter " + setup.label)
    operatorParametersMock.setValue(setup.id, selection)

    class IntegerBoxImpl extends DefaultDialogElementMock(setup.id, setup.label) with IntegerBox

    val de = new IntegerBoxImpl
    updateDialogElements(setup.id, de)
  }

  override def addIntegerBox(id: String, label: String, min: Int, max: Int, defaultValue: Int): IntegerBox = {
    addIntegerBox(new IntegerDialogElementSetup(id,label, isRequired = true, Option.apply(new NumericBound(min, inclusive = true)),
      Option.apply(new NumericBound(max, inclusive = true)), Option.apply(defaultValue.toString)))
  }

  override def addHdfsFileSelector(id: String, label: String, defaultPath: String): HdfsFileSelector = {
    addHdfsFileSelector(id, label, defaultPath, required = true)
  }

  override def addHdfsFileSelector(id: String, label: String, defaultPath: String, required: Boolean): HdfsFileSelector = {
    val selection = setStringValue(id, defaultPath)

    class HdfsFileSelectorImpl extends DefaultDialogElementMock(id, label, required) with HdfsFileSelector
    val de = new HdfsFileSelectorImpl
    updateDialogElements(id, de)
  }

  //toDo: Refactor to use column checkboxes
  override def addTabularDatasetColumnCheckboxes(id: String, label: String,
    columnFilter: ColumnFilter,
    selectionGroupId: String,
    required: Boolean): TabularDatasetColumnCheckboxes = {
    addTabularDatasetColumnCheckboxes(id, label,
      columnFilter,
      selectionGroupId,
      required,
      parentBoxID = None)
  }

  override def addTabularDatasetColumnCheckboxes(id: String, label: String,
    columnFilter: ColumnFilter,
    selectionGroupId: String,
    required: Boolean,
    parentBoxID: Option[String]): TabularDatasetColumnCheckboxes = {

    val columnTypeMap = getTypeMapForColumnSelector(parentBoxID, id)
    val selection: Array[String] = Try(overrideParams.getTabularDatasetSelectedColumns(id)) match {
      case Success(s) =>
        val columns = s._2.filter(_.length > 0)
        columns.map(col => this.validateColumnSelectionAndUpdateSelectionGroup(col, selectionGroupId, columnFilter, columnTypeMap))

      case _ => Array[String]()
    }
    assert(!(required && selection.isEmpty), "Multiple Column Selection Parameter:  " +
      label + " is required. But no values for " +
      " the id  \"" + id + "\" were found.")

    OperatorParameterMockUtil.addTabularColumns(operatorParametersMock, id, selection: _ *)

    class TabularDatasetColumnCheckboxesImpl extends
      AbstractCheckboxMock(availableValues = columnTypeMap.keys.toSeq,
        selected = selection, getId = id, getLabel = label, isRequired = required) with TabularDatasetColumnCheckboxes {}

    val de = new TabularDatasetColumnCheckboxesImpl
    updateDialogElements(id, de)
  }

  override def addDBTableDropdownBox(id: String, label: String, schemaBoxID: String): DBTableDropdownBox = {
    val _schemaBoxID = schemaBoxID
    // Rename to avoid complaint about recursive call.
    // DBTableDropdownBox does not use available values. We need to refactor this to not extend SingleItemSelector.
    class DBTableDropdownBoxImpl extends SingleElementSelectorMock(Seq("mockValue"), "mockValue",
      id, label) with DBTableDropdownBox

    val de = new DBTableDropdownBoxImpl
    updateDialogElements(id, de)
  }

  def addParentOperatorDropdownBox(setup: ParentOperatorSelectorSetup): ParentOperatorDropdownBox = {
    val userSet = overrideParams.getStringValue(setup.id)
    operatorParametersMock.setValue(setup.id, userSet)

    class ParentOperatorDropdownBoxMock extends
      SingleElementSelectorMock(uuids, userSet, setup.id, setup.label) with ParentOperatorDropdownBox
    val de = new ParentOperatorDropdownBoxMock()
    updateDialogElements(setup.id, de)
  }

  override def addParentOperatorDropdownBox(id: String, label: String): ParentOperatorDropdownBox = {
    addParentOperatorDropdownBox(new ParentOperatorSelectorSetup(id, label, isRequired = true))
  }

  override def addParentOperatorDropdownBox(id: String, label: String, required: Boolean): ParentOperatorDropdownBox = {
    addParentOperatorDropdownBox(new ParentOperatorSelectorSetup(id, label, required))
  }

  def addDoubleBox(setup: NumericDialogElementSetup): DoubleBox = {

    val selection: Double = Try(overrideParams.getDoubleValue(setup.id)) match {
      case Success(s) => s
      case _ => setup.defaultValue.get.toDouble
    }
    //toDo: Move this to the

    operatorParametersMock.setValue(setup.id, selection)

    class DoubleBoxImpl extends DefaultDialogElementMock(setup.id, setup.label) with DoubleBox {
      assert(selection >= setup.min.get.value && selection <= setup.max.get.value, "Selection:" +
        selection + " is not in range for integer parameter " + setup.label)
      assert(setup.min.get.inclusive || selection > setup.min.get.value, "Selection : " +
        selection + " is not in range for integer parameter " + setup.label +
        " since the min is exclusive.")
      assert(setup.max.get.inclusive || selection < setup.max.get.value, "Selection : " +
        selection + " is not in range for integer parameter " + setup.label +
        " since the max is exclusive.")
    }

    val de = new DoubleBoxImpl
    updateDialogElements(setup.id, de)
  }

  override def addDoubleBox(id: String, label: String, min: Double, max: Double,
                            inclusiveMin: Boolean, inclusiveMax: Boolean, defaultValue: Double): DoubleBox ={
    addDoubleBox(NumericDialogElementSetup(id, label, isRequired = true,
      Option.apply(NumericBound(min, inclusiveMin)), Some(NumericBound(max, inclusiveMax)),
      Option.apply(defaultValue.toString)))
  }

  def addChorusFileDropdownBox(setup: ChorusFileSelectorSetup): ChorusFileDropdown = {
    class ChorusFileDropdownImpl extends ChorusFileDropdown
    val de = new ChorusFileDropdownImpl()
    operatorParametersMock.setValue(setup.id, overrideParams.getChorusFile(setup.id))
    updateDialogElements(setup.id, de)
  }

  override def addChorusFileDropdownBox(id: String, label: String, extensionFilter: Set[String], isRequired: Boolean): ChorusFileDropdown = {
    addChorusFileDropdownBox(ChorusFileSelectorSetup(id, label, isRequired, extensionFilter, None))
  }

  override def addChorusFileDropdownBox(id: String, label: String, extensionFilter: Set[String], isRequired: Boolean, linkText: Option[String]): ChorusFileDropdown = {
    addChorusFileDropdownBox(ChorusFileSelectorSetup(id, label, isRequired, extensionFilter, linkText))
  }

  override def addAdvancedSparkSettingsBox(id: String, label: String, availableValues: List[SparkParameter]): DialogElement = {
    class AdvancedSparkSettingsBoxImpl extends DefaultDialogElementMock(id, label) with AdvancedSparkSettingsBox
    val de = new AdvancedSparkSettingsBoxImpl
    updateDialogElements(id, de)
  }

  def addScriptEditPopup(setup: ScriptEditPopupSetup): DialogElement = {
    val script = overrideParams.getStringValue(setup.id)
    operatorParametersMock.setValue(setup.id, script)
    class ScriptEditPopupImpl extends DefaultDialogElementMock(setup.id, setup.label) with ScriptEditPopup {
      if (setup.isRequired) {
        assert(script.nonEmpty, s"ScriptEditPopup element ${setup.id} is required, but has empty script.")
      }
    }
    val de = new ScriptEditPopupImpl
    updateDialogElements(setup.id, de)
  }

  override def addScriptEditPopup(id: String, label: String, scriptType: String, required: Boolean): DialogElement = {
    addScriptEditPopup(ScriptEditPopupSetup(id, label, required, scriptType))
  }

  def addRowDialog(setup: RowDialogSetup): DialogElement = {
    val userSetRows = overrideParams.getDialogRowsAsArray(setup.id)
    val nbElementsPerRow = setup.elements.length

    assert(userSetRows.map(_.getRowDialogValues.length).forall(_.equals(nbElementsPerRow)), s"RowDialogSetup ${setup.id} requires $nbElementsPerRow values per row")
    OperatorParameterMockUtil.addRowDialogElements(operatorParametersMock, setup.id, userSetRows:_*)

    class RowDialogImpl extends DefaultDialogElementMock(setup.id, setup.label) with RowDialog {
      if(setup.isRequired) {
        assert(userSetRows.nonEmpty, s"The RowDialogSetup element with id ${setup.id} is required but has no row(s) defined.")
      }
      if (setup.validator.isDefined) {
        val validation = setup.validator.get.validate(userSetRows.toList)
        assert(validation.isValid, s"Validation failed for RowDialogSetup element with id ${setup.id}: " + validation.message)
      }
    }
    val de = new RowDialogImpl
    updateDialogElements(setup.id, de)
  }
}

object OperatorDialogMock {
  @deprecated("Use constructor without tabular schema")
  def apply(params: OperatorParametersMock, input: IOBase, tabularSchema: TabularSchema) =
    new OperatorDialogMock(params, input)

  def apply(params: OperatorParametersMock, input: IOBase) =
    new OperatorDialogMock(params, input)

  @deprecated("Use constructor without tabular schema")
  def apply(params: OperatorParametersMock, input: IOBase, tabularSchema: Option[TabularSchema]) =
    new OperatorDialogMock(params, input)
}


package com.alpine.plugin.test.mock

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog._
import com.alpine.plugin.core.io.{ColumnDef, IOBase, TabularSchema}
import com.alpine.plugin.test.utils.OperatorParameterMockUtil

import scala.collection.JavaConversions._
import scala.util.{Success, Try}

abstract class DefaultDialogElementMock(override val getId: String,
                                        override val getLabel: String,
                                        override val isRequired: Boolean = true) extends DialogElement {}

abstract class SingleElementSelectorMock(availableValues: Seq[String],
                                         override val getSelectedValue: String,
                                         override val getId: String,
                                         override val getLabel: String,
                                         override val isRequired: Boolean = true) extends SingleItemSelector {

  assert(availableValues.distinct.length.equals(availableValues.length),
    "The available values for the parameters " + getLabel + "are not distinct")
  assert(availableValues.contains(getSelectedValue), "The value " + getSelectedValue +
    " is not one of the available values for the parameter " + getLabel)

  override def addValue(value: String): Unit = {}

  override def setSelectedValue(value: String): Unit = {}

  override def getValues: Iterator[String] = availableValues.toIterator
}

abstract class AbstractCheckboxMock(availableValues: Seq[String],
                                    selected: Seq[String],
                                    override val getId: String,
                                    override val getLabel: String,
                                    override val isRequired: Boolean = true) extends Checkboxes {

  //validation logic
  selected.foreach(v =>
    assert(availableValues.contains(v), "The value " + v +
      " is not one of the available values for the parameter " + getLabel))
  assert(availableValues.distinct.length == availableValues.length,
    "The available values for the parameters " + getLabel + "are not distinct")
  override val getValues: Iterator[String] = availableValues.toIterator

  override def setCheckboxSelection(value: String, selected: Boolean): Unit = {
  }

  override def addValue(value: String): Unit = {}

  override def getSelectedValues: Iterator[String] = selected.toIterator
}

class OperatorDialogMock(overrideParams: OperatorParametersMock,
                         input: IOBase,
                         inputSchema: Option[TabularSchema]) extends OperatorDialog {

  private val workfileMap = overrideParams.chorusAPICaller.workfileMap

  private val dialogElements = scala.collection.mutable.Map[String, DialogElement]()
  private val selectionGroupIdMap = scala.collection.mutable.Map[String, Set[String]]()
  private val nameTypeMap = inputSchema.getOrElse(TabularSchema(Seq())).getDefinedColumns.map(x => (x.columnName, x.columnType)).toMap
  private val inputColumns = nameTypeMap.keySet

  private val operatorParametersMock = new OperatorParametersMock(
    overrideParams.operatorInfo().name,
    overrideParams.operatorInfo().uuid
  )

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

  private def validateColumnSelectionAndUpdateSelectionGroup(
    column: String, selectionGroupId: String, filter: ColumnFilter): String = {
    assert(inputColumns.contains(column), "Column: " + column + " is not present in input schema.")

    val group = selectionGroupIdMap.getOrElseUpdate(selectionGroupId, Set())
    assert(!group.contains(column), "The selection group id: " + selectionGroupId +
      " already contains the column " + column)
    assert(column.matches(filter.acceptedNameRegex))
    val t = nameTypeMap.get(column).get
    assert(filter.accepts(ColumnDef(column, t)), "The column, " + column + " has type " + t.name +
      " which is not one of the accepted type for the column filter which accepts: ")
    selectionGroupIdMap.update(selectionGroupId, group.union(Set(column)))
    column
  }

  /**
    * Returns a new mock parameters object with all the parameters that were added to this dialog object.
    */
  def getNewParameters = operatorParametersMock

  override def getLabel(): String = "Label"

  override def addStringBox(id: String, label: String, defaultValue: String, regex: String, width: Int, height: Int): StringBox = {
    addStringBox(id, label, defaultValue, regex, required = true)
  }

  override def addStringBox(id: String, label: String, defaultValue: String, regex: String, required: Boolean): StringBox = {
    val selected = setStringValue(id, defaultValue)

    class StringBoxImpl extends DefaultDialogElementMock(id, label, required) with StringBox {
      // var value : String = null
      assert(getValue.matches(getRegex), "The string: " + getValue + "does not conform to regex: " + regex)

      override def setValue(v: String): Unit = {}

      override def getValue: String = selected

      override def getRegex: String = regex
    }

    val de = new StringBoxImpl()
    updateDialogElements(id, de)
  }

  override def addLargeStringBox(id: String, label: String, defaultValue: String, regex: String, required: Boolean): StringBox = {
    addStringBox(id, label, defaultValue, regex, required)
  }

  override def getDialogElement(id: String): DialogElement = dialogElements.get(id).get

  override def getDialogElements(): Seq[DialogElement] = dialogElements.values.toSeq

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

  override def addDropdownBox(id: String, label: String, values: Seq[String], defaultSelection: String): DropdownBox = {
    val s = setStringValue(id, defaultSelection)
    class DropDownBoxImpl extends SingleElementSelectorMock(values, s, id, label) with
    DropdownBox {
    }
    //toDo: ditch this sequence, do validation at object creation --> less mutable
    val de = new DropDownBoxImpl
    updateDialogElements(id, de)
  }

  override def addDropdownBoxFromJavaList(id: String, label: String, values: java.util.List[String], defaultSelection: String): DropdownBox = {
    addDropdownBox(id, label, asScalaBuffer(values).toList, defaultSelection)
  }

  override def addHdfsDirectorySelector(id: String, label: String, defaultPath: String): HdfsFileSelector = {
      addHdfsDirectorySelector(id, label, defaultPath, required = true)
  }

  override def addHdfsDirectorySelector(id: String, label: String, defaultPath: String, required : Boolean): HdfsFileSelector = {
    val path = setStringValue(id, defaultPath)

    class HdfsFileSelectorImp extends DefaultDialogElementMock(id, label, required) with HdfsFileSelector {
      private var p: String = null

      override def getSelectedPath: String = p

      override def setSelectedPath(path: String): Unit = {
        p = path
      }

      override def isDirectorySelector: Boolean = true
    }

    val de = new HdfsFileSelectorImp
    de.setSelectedPath(path)
    updateDialogElements(id, de)
  }


  override def addDataSourceDropdownBox(id: String, label: String,
                                        dataSourceManager: OperatorDataSourceManager): DataSourceDropdownBox = {
    val selectedValue = setStringValue(id, dataSourceManager.getRuntimeDataSource().getName)
    val allSources = dataSourceManager.getDataSources.map(_.getName)
    class DataSourceDropdownBoxImpl extends
    SingleElementSelectorMock(allSources.toSeq, selectedValue, id,
      dataSourceManager.getRuntimeDataSource().getName) with DataSourceDropdownBox {
    }

    val de = new DataSourceDropdownBoxImpl
    updateDialogElements(id, de)
  }


  override def addTabularDatasetColumnDropdownBox(id: String, label: String,
                                                  columnFilter: ColumnFilter,
                                                  selectionGroupId: String,
                                                  required: Boolean): TabularDatasetColumnDropdownBox = {
    assert(inputSchema.isDefined, "Cannot create tabular dataset column drop down " + label + " for " +
      "non tabular input")

    val selection: String = Try(overrideParams.getTabularDatasetSelectedColumn(id)) match {
      case Success(s) =>
        val column = s._2
        validateColumnSelectionAndUpdateSelectionGroup(column, selectionGroupId, columnFilter)
      case _ => ""
    }

    assert(!(required && selection.equals("")))

    OperatorParameterMockUtil.addTabularColumn(operatorParametersMock, id, selection)


    class TabularDatasetColumnDropdownBoxImpl extends
    SingleElementSelectorMock(availableValues = inputColumns.toSeq, getSelectedValue = selection,
      getId = id, getLabel = label, isRequired = required) with
    TabularDatasetColumnDropdownBox {
      assert(inputColumns.contains(selection),
        "The input of this operator does not contain the column " + selection +
          "in the input schema.")
    }

    val de = new TabularDatasetColumnDropdownBoxImpl
    updateDialogElements(id, de)
  }

  override def addDBSchemaDropdownBox(id: String, label: String, defaultSchema: String): DBSchemaDropdownBox = {
    val selected = setStringValue(id, defaultSchema)
    class DBSchemaDropdownBoxImpl extends SingleElementSelectorMock(Seq(selected), selected, id, label)
    with DBSchemaDropdownBox {}

    val de = new DBSchemaDropdownBoxImpl
    updateDialogElements(id, de)

  }

  override def addRadioButtons(id: String, label: String, values: Seq[String], defaultSelection: String): RadioButtons = {
    val selected = setStringValue(id, defaultSelection)

    class RadioButtonImpl extends SingleElementSelectorMock(values, selected, id, label) with RadioButtons

    val de = new RadioButtonImpl
    updateDialogElements(id, de)
  }

  override def addRadioButtonsFromJavaList(id: String, label: String, values: java.util.List[String], defaultSelection: String): RadioButtons = {
    addRadioButtons(id, label, asScalaBuffer(values).toList, defaultSelection)
  }

  override def addIntegerBox(id: String, label: String, min: Int, max: Int, defaultValue: Int): IntegerBox = {
    val selection = Try(overrideParams.getIntValue(id)) match {
      case Success(s) => s
      case _ => defaultValue
    }
    require(selection >= min || selection <= max, "Selection:" + selection + " is not in range for integer parameter " + label)
    operatorParametersMock.setValue(id, selection)

    class IntegerBoxImpl extends DefaultDialogElementMock(id, label) with IntegerBox {

      override def getValue: Int = selection

      override def getMin: Int = min

      override def getMax: Int = max
    }

    val de = new IntegerBoxImpl
    updateDialogElements(id, de)
  }

  override def addHdfsFileSelector(id: String, label: String, defaultPath: String): HdfsFileSelector = {
    addHdfsFileSelector(id, label, defaultPath, required = true)
  }

  override def addHdfsFileSelector(id: String, label: String, defaultPath: String, required : Boolean): HdfsFileSelector = {
    val selection = setStringValue(id, defaultPath)

    class HdfsFileSelectorImpl extends DefaultDialogElementMock(id, label, required) with HdfsFileSelector {

      override def getSelectedPath: String = selection

      override def setSelectedPath(path: String): Unit = {}

      override def isDirectorySelector: Boolean = false
    }

    val de = new HdfsFileSelectorImpl
    updateDialogElements(id, de)
  }

  //toDo: Refactor to use column checkboxes
  override def addTabularDatasetColumnCheckboxes(id: String, label: String,
                                                 columnFilter: ColumnFilter,
                                                 selectionGroupId: String,
                                                 required: Boolean): TabularDatasetColumnCheckboxes = {

    assert(inputSchema.isDefined, "Cannot create tabular dataset column dropdown " + label + " for " +
      "non tabular input")

    val selection: Array[String] = Try(overrideParams.getTabularDatasetSelectedColumns(id)) match {
      case Success(s) =>
        val columns = s._2.filter(_.length > 0)
        columns.map(col => this.validateColumnSelectionAndUpdateSelectionGroup(col, selectionGroupId, columnFilter))

      case _ => Array[String]()
    }
    assert(!(required && selection.isEmpty), "Multiple Column Selection Parameter:  " +
      label + " is required. But no values for " +
      " the id  \"" + id + "\" were found.")

    OperatorParameterMockUtil.addTabularColumns(operatorParametersMock, id, selection: _ *)

    class TabularDatasetColumnCheckboxesImpl extends AbstractCheckboxMock(availableValues =inputColumns.toSeq,
      selected = selection, getId = id, getLabel = label, isRequired = required) with TabularDatasetColumnCheckboxes {}

    val de = new TabularDatasetColumnCheckboxesImpl
    updateDialogElements(id, de)
  }

  override def addDBTableDropdownBox(id: String, label: String, schemaBoxID: String): DBTableDropdownBox = {
    // DBTableDropdownBox does not use available values. We need to refactor this to not extend SingleItemSelector.
    class DBTableDropdownBoxImpl extends SingleElementSelectorMock(Seq("mockValue"), "mockValue",
      id, label) with DBTableDropdownBox {
      override def schemaBoxID: String = schemaBoxID
    }

    val de = new DBTableDropdownBoxImpl
    updateDialogElements(id, de)
  }

  override def addParentOperatorDropdownBox(id: String, label: String): ParentOperatorDropdownBox = null

  override def addDoubleBox(id: String, label: String, min: Double, max: Double,
                            inclusiveMin: Boolean, inclusiveMax: Boolean, defaultValue: Double): DoubleBox = {

    val selection: Double = Try(overrideParams.getDoubleValue(id)) match {
      case Success(s) => s
      case _ => defaultValue
    }
    //toDo: Move this to the

    operatorParametersMock.setValue(id, selection)

    class DoubleBoxImpl extends DefaultDialogElementMock(id, label) with DoubleBox {
      assert(getValue >= getMin && getValue <= getMax, "Selection:" +
        getValue + " is not in range for integer parameter " + label)
      assert(getInclusiveMin || getValue > getMin, "Selection : " +
        getValue + " is not in range for integer parameter " + label +
        " since the min is exclusive.")
      assert(getInclusiveMax || getValue < getMax, "Selection : " +
        getValue + " is not in range for integer parameter " + label +
        " since the max is exclusive.")

      override def getValue: Double = selection

      override def getInclusiveMin: Boolean = inclusiveMin

      override def getMin: Double = min

      override def getInclusiveMax: Boolean = inclusiveMax

      override def getMax: Double = max
    }

    val de = new DoubleBoxImpl
    updateDialogElements(id, de)
  }

  override def addChorusFileDropdownBox(id: String, label: String, extensionFilter: Set[String], isRequired: Boolean): ChorusFileDropdown = {
    val availableValues = this.workfileMap.values.map(w => w.wf.fileName).toSeq

    val param = overrideParams.getChorusFile(id)
    if (isRequired)
      assert(this.workfileMap.contains(param.fileId),
        "Chorus file with name: " + param.fileName + ", and id " + param.fileId + " is not in the workfile map provided")
    class ChorusFileDropdownImpl extends SingleElementSelectorMock(availableValues = availableValues,
      getSelectedValue = param.fileName,
      getId = label, getLabel = label) with ChorusFileDropdown {
      //TODO: Add logic to check that the parameter value provided has the correct extension given the extension filter.
      override def getExtensionFilter: Seq[String] = extensionFilter.toSeq
    }
    val de = new ChorusFileDropdownImpl()

    operatorParametersMock.setValue(id, param)
    updateDialogElements(id, de)
  }

  override def addAdvancedSparkSettingsBox(id: String, label: String, availableValues: List[SparkParameter]): DialogElement = {
    class AdvancedSparkSettingsBoxImpl extends DefaultDialogElementMock(id, label) with AdvancedSparkSettingsBox {

      override def setAvailableValues(options: Iterator[String]): Unit = {}

      override def getSetting(settingId: String): String = {

        val element: String = Try(availableValues.find(t => t.name == settingId).head) match {
          case Success(s) => s.value
          case _ => ""
        }
        element
      }

      override def addSetting(name: String, value: String): Unit = {}

      override def setParameters(options: Iterator[SparkParameter]): Unit = {}

      override def setAvailableValues(options: Map[String, String]): Unit = {}
    }

    val de = new AdvancedSparkSettingsBoxImpl
    updateDialogElements(id, de)
  }

}

object OperatorDialogMock {
  def apply(params: OperatorParametersMock, input: IOBase, tabularSchema: TabularSchema) =
    new OperatorDialogMock(params, input, Some(tabularSchema))

  def apply(params: OperatorParametersMock, input: IOBase) =
    new OperatorDialogMock(params, input, None)
}


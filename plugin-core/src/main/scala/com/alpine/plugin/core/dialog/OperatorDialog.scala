/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.dialog

import java.util.regex.Pattern

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
  * :: AlpineSdkApi ::
  * This represents the operator parameter dialog window. The operator can add
  * input UIs during registration.
  */
@AlpineSdkApi
trait OperatorDialog {
  /**
    * Get the label for this dialog box.
    *
    * @return Get the label.
    */
  def getLabel: String

  /**
    * Get an iterator of dialog elements.
    *
    * @return The dialog elements.
    */
  def getDialogElements: Seq[DialogElement]

  /**
    * Get a dialog element by the Id.
    *
    * @param id The id of the dialog element that we want to retrieve.
    * @return The matching dialog element.
    */
  def getDialogElement(id: String): DialogElement

  def addAdvancedSparkSettingsBox(id: String, label: String, availableValues: List[SparkParameter]): DialogElement

  /**
    * Add a data source selection box. This can be used to select a particular
    * data source if the workflow is associated with multiple data sources.
    *
    * @param id                String id of this box. This is used later to reference the value
    *                          of this input box.
    * @param label             This visual label of this input box.
    * @param dataSourceManager The data source manager that contains the information
    *                          about the available data sources as well as the
    *                          chosen data source.
    * @throws Exception It could throw an exception if the user tried to add
    *                   more than one data source selection box to an operator
    *                   dialog object.
    * @return A data source dropdown box element.
    */
  @throws(classOf[Exception])
  def addDataSourceDropdownBox(id: String,
                               label: String,
                               dataSourceManager: OperatorDataSourceManager
                              ): DataSourceDropdownBox

  /**
    * Add a Hdfs directory selection box. This can be used to select an Hdfs
    * directory that already exists through an interactive selection window.
    *
    * @param id          String id of this input box. This is used later to reference
    *                    the value of this input box.
    * @param label       The visual label of this input box.
    * @param defaultPath The default value to be used in the input box.
    * @return A Hdfs directory selection box element.
    */
  def addHdfsDirectorySelector(id: String,
                               label: String,
                               defaultPath: String
                              ): HdfsFileSelector

  /**
    * Add a Hdfs directory selection box. This can be used to select an Hdfs
    * directory that already exists through an interactive selection window.
    *
    * @param id          String id of this input box. This is used later to reference
    *                    the value of this input box.
    * @param label       The visual label of this input box.
    * @param defaultPath The default value to be used in the input box.
    * @param isRequired  - If the operator can be valid if this is left blank
    * @return A Hdfs directory selection box element.
    */
  def addHdfsDirectorySelector(id: String,
                               label: String,
                               defaultPath: String,
                               isRequired: Boolean): HdfsFileSelector

  /**
    * Add a Hdfs file selection box. This can be used to select an Hdfs file
    * that already exists through an interactive selection window.
    *
    * @param id          String id of this input box. This is used later to reference
    *                    the value of this input box.
    * @param label       The visual label of this input box.
    * @param defaultPath The default value to be used in the input box.
    * @return A Hdfs file selection box element.
    */
  def addHdfsFileSelector(id: String,
                          label: String,
                          defaultPath: String
                         ): HdfsFileSelector

  /**
    * Add a Hdfs file selection box. This can be used to select an Hdfs file
    * that already exists through an interactive selection window.
    *
    * @param id          String id of this input box. This is used later to reference
    *                    the value of this input box.
    * @param label       The visual label of this input box.
    * @param defaultPath The default value to be used in the input box.
    * @param isRequired  - If the operator can be valid if this is left blank
    * @return A Hdfs file selection box element.
    */
  def addHdfsFileSelector(id: String,
                          label: String,
                          defaultPath: String,
                          isRequired: Boolean
                         ): HdfsFileSelector

  /**
    * Add a schema selection box. This can be used to select the schema for
    * the output table location, etc. This will be a dropdown menu.
    *
    * @param id            String id of this input box. This is used later to reference
    *                      the value of this input box.
    * @param label         The visual label of this input box.
    * @param defaultSchema The default value to be used in the input box.
    * @return A DB schema dropdown box element.
    */
  def addDBSchemaDropdownBox(id: String,
                             label: String,
                             defaultSchema: String
                            ): DBSchemaDropdownBox

  /**
    * Add a table selection box. This can be used to select an existing table for
    * an input or an output table.
    *
    * @param id          String id of this input box. This is used later to reference
    *                    the value of this input box.
    * @param label       The visual label of this input box.
    * @param schemaBoxID The id of the schema dialog element, to know which
    * @return A DB table dropdown box element.
    */
  def addDBTableDropdownBox(id: String, label: String, schemaBoxID: String): DBTableDropdownBox

  /**
    * @param id              String id of dropdown box. This corresponds to the id for this parameter in OperatorsParameters.
    * @param label           The visual label of this dropdown box.
    * @param extensionFilter e.g. Seq(.pynb) for python notebooks
    */
  def addChorusFileDropdownBox(id: String, label: String, extensionFilter: Set[String], isRequired: Boolean): ChorusFileDropdown

  /**
    * Add an integer text box.
    *
    * @param id           String id of this input box.
    * @param label        The visual label of this input box.
    * @param min          The minimum accepted value for the integer.
    * @param max          The maximum accepted value for the integer.
    * @param defaultValue The default value for the integer.
    * @return An integer box element.
    */
  def addIntegerBox(id: String,
                    label: String,
                    min: Int,
                    max: Int,
                    defaultValue: Int
                   ): IntegerBox

  /**
    * Add a double text box.
    *
    * @param id           String id of this input box.
    * @param label        The visual label of this input box.
    * @param min          The minimum value.
    * @param max          The maximum value.
    * @param inclusiveMin Whether the minimum is an inclusive value.
    * @param inclusiveMax Whether the maximum is an inclusive value.
    * @param defaultValue The default value for the double.
    * @return A double box element.
    */
  def addDoubleBox(id: String,
                   label: String,
                   min: Double,
                   max: Double,
                   inclusiveMin: Boolean,
                   inclusiveMax: Boolean,
                   defaultValue: Double
                  ): DoubleBox

  /**
    * Add a button that opens a multiple checkbox dialog box.
    *
    * @param id                String id of this input box.
    * @param label             The visual label of this input box.
    * @param values            Available checkbox values.
    * @param defaultSelections Default selected checkboxes.
    * @param required          Whether the user is required to select a value for this parameter.
    * @return A checkboxes element.
    */
  def addCheckboxes(id: String,
                    label: String,
                    values: Seq[String],
                    defaultSelections: Seq[String],
                    required: Boolean = true
                   ): Checkboxes

  /**
    * Add a button that opens a multiple checkbox dialog box.
    * This version is Java-friendly
    *
    * @param id                String id of this input box.
    * @param label             The visual label of this input box.
    * @param values            Available checkbox values as java.util.List
    * @param defaultSelections Default selected checkboxes.
    * @param required          Whether the user is required to select a value for this parameter.
    * @return A checkboxes element.
    */
  def addCheckboxesFromJavaList(id: String,
                                label: String,
                                values: java.util.List[String],
                                defaultSelections: java.util.List[String],
                                required: Boolean = true
                               ): Checkboxes

  /**
    * Add a radio button input (a multiple choice input).
    *
    * @param id               String id of this input box.
    * @param label            The visual label of this input box.
    * @param values           Available checkbox values
    * @param defaultSelection Default selected button.
    * @return A radio button element.
    */
  def addRadioButtons(id: String,
                      label: String,
                      values: Seq[String],
                      defaultSelection: String
                     ): RadioButtons

  /**
    * Add a radio button input (a multiple choice input).
    * This version is Java-friendly.
    *
    * @param id               String id of this input box.
    * @param label            The visual label of this input box.
    * @param values           Available checkbox values as java.util.List
    * @param defaultSelection Default selected button.
    * @return A radio button element.
    */
  def addRadioButtonsFromJavaList(id: String,
                                  label: String,
                                  values: java.util.List[String],
                                  defaultSelection: String
                                 ): RadioButtons

  /**
    * Add a dropdown menu (a multiple choice input).
    *
    * @param id               String id of this input box.
    * @param label            The visual label of this input box.
    * @param values           Available checkbox values.
    * @param defaultSelection Default selected vale.
    * @return A dropdown box element.
    */
  def addDropdownBox(id: String,
                     label: String,
                     values: Seq[String],
                     defaultSelection: String
                    ): DropdownBox

  /**
    * Add a dropdown menu (a multiple choice input).
    * This version is Java-friendly.
    *
    * @param id               String id of this input box.
    * @param label            The visual label of this input box.
    * @param values           Available checkbox values from java.util.List
    * @param defaultSelection Default selected vale.
    * @return A dropdown box element.
    */
  def addDropdownBoxFromJavaList(id: String,
                                 label: String,
                                 values: java.util.List[String],
                                 defaultSelection: String
                                ): DropdownBox

  /**
    * Add a string input box.
    *
    * @param id           String id of this input box.
    * @param label        The visual label of this input box.
    * @param defaultValue The default value in the string box.
    * @param regex        The regular expression constraint for the input box.
    * @param width        Number of pixels for the width. 0 will use a default value.
    * @param height       Number of pixels for the height. 0 will use a default value.
    * @return A string box element.
    */
  @deprecated("Use addStringBox (without width and height parameters) or addLargeStringBox instead.")
  def addStringBox(id: String,
                   label: String,
                   defaultValue: String,
                   regex: String,
                   width: Int,
                   height: Int
                  ): StringBox

  /**
    * Add a string input box.
    *
    * @param id           String id of this input box.
    * @param label        The visual label of this input box.
    * @param defaultValue The default value in the string box.
    * @param regex        The regular expression constraint for the input box.
    * @param required     Boolean indicating whether the value must be non-empty.
    * @return A string box element.
    */
  def addStringBox(id: String,
                   label: String,
                   defaultValue: String,
                   regex: String,
                   required: Boolean
                  ): StringBox

  /**
    * Add a large string input box.
    *
    * @param id           String id of this input box.
    * @param label        The visual label of this input box.
    * @param defaultValue The default value in the string box.
    * @param regex        The regular expression constraint for the input box.
    * @param required     Boolean indicating whether the value must be non-empty.
    * @return A string box element.
    */
  def addLargeStringBox(id: String,
                        label: String,
                        defaultValue: String,
                        regex: String,
                        required: Boolean
                       ): StringBox

  /**
    * Add a drop down box that can be used to select a parent operator name.
    * This is useful to point to a particular parent's output for a particular
    * use.
    *
    * Equivalent to [[addParentOperatorDropdownBox(id, label, true)]]
    *
    * @param id    String id of this input box.
    * @param label The visual label of this input box.
    * @return A parent operator dropdown box.
    */
  def addParentOperatorDropdownBox(id: String,
                                   label: String
                                  ): ParentOperatorDropdownBox

  /**
    * Add a drop down box that can be used to select a parent operator name.
    * This is useful to point to a particular parent's output for a particular
    * use.
    *
    * @param id       String id of this input box.
    * @param label    The visual label of this input box.
    * @param required Whether the user is required to select a value for this parameter.
    * @return A parent operator dropdown box.
    */
  def addParentOperatorDropdownBox(id: String,
                                   label: String,
                                   required: Boolean
                                  ): ParentOperatorDropdownBox

  /**
    * Add a button for column checkboxes for a dataset input.
    * This can be used to select multiple columns from a tabular dataset
    * input. I.e., this will match tabular datasets coming in as inputs.
    * In case of multiple dataset inputs, there'll be a separate
    * column selector per dataset input.
    *
    * @param id               String id for this parameter set.
    * @param label            The label (prefix) for this parameter set. In case there are
    *                         multiple input datasets, each column selector button will be
    *                         prefixed by this label.
    * @param columnFilter     Filter the columns that are selectable by their types.
    * @param selectionGroupId If we want the available selections in this group
    *                         to be dependent on other column selectors such
    *                         that there's no duplicate selections, one could
    *                         put multiple column selectors (checkboxes and/or
    *                         dropboxes) in the same group.
    * @param required         Whether the user is required to select a value for this parameter.
    * @return An input column checkboxes element.
    */
  def addTabularDatasetColumnCheckboxes(id: String,
                                        label: String,
                                        columnFilter: ColumnFilter,
                                        selectionGroupId: String,
                                        required: Boolean = true
                                       ): TabularDatasetColumnCheckboxes

  /**
    * Add a column selector dropdown box for a dataset input.
    * This can be used to select a single column from a tabular dataset.
    * In case of multiple tabular dataset inputs, there'll be a separate
    * column selector per dataset input.
    *
    * @param id               String id for this parameter set.
    * @param label            The label (prefix) for this parameter set. In case there are
    *                         multiple input datasets, each column selector button will be
    *                         prefixed by this label.
    * @param columnFilter     Filter the columns that are selectable by their types.
    * @param selectionGroupId If we want the available selections in this group
    *                         to be dependent on other column selectors such
    *                         that there's no duplicate selections, one could
    *                         put multiple column selectors (checkboxes and/or
    *                         dropboxes) in the same group.
    * @param required         Whether the user is required to select a value for this parameter.
    * @return A single column selector dropdown box element.
    */
  def addTabularDatasetColumnDropdownBox(
                                          id: String,
                                          label: String,
                                          columnFilter: ColumnFilter,
                                          selectionGroupId: String,
                                          required: Boolean = true
                                        ): TabularDatasetColumnDropdownBox
}

/**
  * :: AlpineSdkApi ::
  * A column filter is a collection of accepted column types and names.
  * A special type '*' means all the types are accepted by the filter.
  * The name acceptance is handled via a regular expression.
  *
  * @param acceptedTypes     Accepted type set.
  * @param acceptedNameRegex Regular expression for accepted column names.
  */
case class ColumnFilter(acceptedTypes: Set[ColumnType.TypeValue],
                        acceptedNameRegex: String = ".+"
                       ) {


  /**
    * :: AlpineSdkApi ::
    * Determine whether the given column is accepted by the filter.
    *
    * @param colDef The definition of the column we want to check for acceptance.
    */
  def accepts(colDef: ColumnDef): Boolean = {
    val colType = colDef.columnType
    val typeAccepted =
      if (acceptedTypes.contains(ColumnType.TypeValue(ColumnFilter.ALL_TYPES))) {
        true
      } else if (colType.name == ColumnType.DateTime.name &&
        acceptedTypes.contains(ColumnType.TypeValue(ColumnFilter.ALL_DATE_TYPES))) {
        true
      }
      else {
        acceptedTypes.contains(colType)
      }
    val p = Pattern.compile(acceptedNameRegex)
    val m = p.matcher(colDef.columnName)
    val nameAccepted = m.matches()

    typeAccepted && nameAccepted
  }
}

/**
  * :: AlpineSdkApi ::
  * A companion filter object that's used to create off-shelf filters.
  */
//TODO: Refactor to allow date column filter
object ColumnFilter {
  val ALL_TYPES = "*"
  val ALL_DATE_TYPES = "*dates"

  /**
    * :: AlpineSdkApi ::
    * Return a filter that only passes numeric types, if the columns are coming
    * from a Hadoop dataset. This doesn't necessarily work for DB columns, which
    * often have vendor-specific types.
    *
    * @return A column filter that accepts numeric types (for Hadoop datasets).
    */
  def NumericOnly: ColumnFilter = {
    ColumnFilter(
      Set[ColumnType.TypeValue](
        ColumnType.Int,
        ColumnType.Long,
        ColumnType.Float,
        ColumnType.Double
      )
    )
  }

  /**
    * :: AlpineSdkApi ::
    * Return a filter that only passes categorical types, if the columns are
    * coming from a Hadoop dataset. This doesn't necessarily work for DB columns,
    * which often have vendor-specific types.
    *
    * @return A column filter that accepts categorical types (for Hadoop datasets).
    */
  def CategoricalOnly: ColumnFilter = {
    ColumnFilter(
      Set[ColumnType.TypeValue](
        ColumnType.String
      )
    )
  }

  /**
    * :: AlpineSdkApi ::
    * Return a filter that only passes categorical types, if the columns are
    * coming from a Hadoop dataset. This doesn't necessarily work for DB columns,
    * which often have vendor-specific types.
    *
    * @return A column filter that accepts categorical types (for Hadoop datasets).
    */
  def DateTimeOnly: ColumnFilter = {
    ColumnFilter(
      Set[ColumnType.TypeValue](
        ColumnType.TypeValue(ALL_DATE_TYPES)
      )
    )
  }

  /**
    * :: AlpineSdkApi ::
    * Return a filter that accepts any type.
    *
    * @return A column filter that accepts any type.
    */
  def All: ColumnFilter = {
    ColumnFilter(
      Set[ColumnType.TypeValue](
        ColumnType.TypeValue(ALL_TYPES) // A special type representing all.
      )
    )
  }

  /**
    * :: AlpineSdkApi ::
    * Get a customized filter that accepts the user specified types.
    *
    * @param acceptedNameRegex A regular expression for accepted column names.
    * @param acceptedTypes     A variable argument list that contains all the
    *                          accepted types.
    * @return A customized filter.
    */
  def apply(acceptedNameRegex: String,
            acceptedTypes: ColumnType.TypeValue*): ColumnFilter = {
    ColumnFilter(acceptedTypes.toSet, acceptedNameRegex)
  }
}

object Regex {
  val ANY_STRING = ".*"
  val AT_LEAST_ONE_CHAR = ".+"
  val VALID_COLUMN_NAME = "^[A-Za-z]+\\w*$"
}

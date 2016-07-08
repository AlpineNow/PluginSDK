/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{DialogElement, OperatorDialog}

/**
 * This is a utility class which handles adding output parameters which are standard
 * to Hive and Database plugins.
 */
trait TableOutputParameterUtils extends OutputParameterUtils {

  /**
    * Static values which will be used as parameter keys
    */
  val defaultTableName = "alp@user_id_@flow_id_" + operatorNameUUIDVariable
  val resultTableNameParameterID = "resultTableName"
  val defaultDatabaseName = "@default_schema"
  val outputSchemaParameterId = "outputSchema"
  val viewOrTableParameterKey = "viewOrTable"
  val viewKey = "VIEW"
  val tableKey = "TABLE"

  def addResultTableNameParameter(operatorDialog: OperatorDialog,
                                  defaultTableName: String = defaultTableName,
                                  label: String = "Result Table Name"): DialogElement = {
    val tableName = operatorDialog.addStringBox(resultTableNameParameterID, label, defaultTableName, ".+", required = true)
    tableName
  }

  def addViewOrTableRadioButton(operatorDialog: OperatorDialog): DialogElement = {
    operatorDialog.addRadioButtons(
      viewOrTableParameterKey,
      "Output Type",
      Seq(tableKey, viewKey),
      tableKey
    )
  }

  /**
    * Use in the GUINode:
    * Adds the schema drop down box, "outputSchema" to the operatorDialog and returns
    * the DialogElement
    */
  @deprecated
  def addDBSchemaDropDown(operatorDialog: OperatorDialog,
                          operatorDataSourceManager: OperatorDataSourceManager,
                          defaultSchema: String = defaultDatabaseName): DialogElement = {
    addDBSchemaDropDownBox(operatorDialog, defaultSchema)
  }

  /**
    * Use in the GUINode:
    * Adds the schema drop down box, "outputSchema" to the operatorDialog and returns
    * the DialogElement
    */
  def addDBSchemaDropDownBox(operatorDialog: OperatorDialog,
                             defaultSchema: String = defaultDatabaseName): DialogElement = {
    operatorDialog.addDBSchemaDropdownBox(outputSchemaParameterId, "Output Schema", defaultSchema)
  }

  /**
   * Gets the value of the table name parameter.
   * Note: In in PostgresSQL database names containing capital letters will need to be double quoted.
   * @param parameters OperatorParameters instance [containing the "resultTableName" parameter].
   * @throws java.lang.NullPointerException if the parameter wasn't added to the parameters object
   * @return the output table name
   */
  @throws(classOf[NullPointerException])
  def getResultTableName(parameters: OperatorParameters): String = {
    parameters.getStringValue(resultTableNameParameterID)
  }

  /**
   * Use in the runtime class:
   * Gets the value of "isViewOrTable"  parameter  and returns true if the user set the
   * radio button to "view"
   * @param parameters OperatorParameters instance [containing the "viewOrTable" parameter].
   * @throws java.lang.NullPointerException if the parameter wasn't added to the parameters object
   * @return if the user selected "view"
   */
  @throws(classOf[NullPointerException])
  def getIsViewParam(parameters: OperatorParameters): Boolean = {
    val viewOrTable = parameters.getStringValue(viewOrTableParameterKey)
    viewOrTable != null && viewOrTable.toUpperCase == viewKey // viewKey is now VIEW
  }

}

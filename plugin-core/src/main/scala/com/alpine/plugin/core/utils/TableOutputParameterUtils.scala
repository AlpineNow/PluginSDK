/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog.{DialogElement, OperatorDialog}

/**
 * This is a utility class which handles adding output parameters which are standard
 * to Hive and Database plugins.
 */
trait TableOutputParameterUtils extends OutputParameterUtils{

  val defaultTableName = "alp@user_id_@flow_id_" + operatorNameUUIDVariable
  val resultTableNameParameterID = "resultTableName"
  val defaultDatabaseName = "@default_schema"
  val viewOrTableParameterKey = "viewOrTable"
  val viewKey = "view"
  val tableKey = "table"

  def addResultTableNameParameter(operatorDialog: OperatorDialog,
                                  defaultTableName: String = defaultTableName): DialogElement = {
    val tableName = operatorDialog.addStringBox(
      resultTableNameParameterID, "Result Table Name", defaultTableName, ".+", 0, 0
    )
    tableName
  }

  def addViewOrTableRadioButton(operatorDialog: OperatorDialog): DialogElement = {
    operatorDialog.addRadioButtons(viewOrTableParameterKey,
      "Output Type ", Seq(viewKey, tableKey), tableKey)
  }

  /**
   * Gets the value of the table name parameter.
   * Note: In in Postgress databases names containing capital letters will need to be double quoted
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
     @param parameters OperatorParameters instance [containing the "viewOrTable" parameter].
   * @throws java.lang.NullPointerException if the parameter wasn't added to the parameters object
   * @return if the user selected "view"
   */
  @throws(classOf[NullPointerException])
  def getIsViewParam(parameters: OperatorParameters): Boolean = {
    parameters.getStringValue(viewOrTableParameterKey) == viewKey
  }

}

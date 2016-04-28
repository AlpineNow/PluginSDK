/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{DialogElement, OperatorDialog}

object DBParameterUtils extends TableOutputParameterUtils {

  @deprecated
  def addStandardDatabaseOutputParameters(operatorDialog: OperatorDialog,
                                          dataSourceManager: OperatorDataSourceManager,
                                          defaultOutputName: String = operatorNameUUIDVariable
                                         ): Seq[DialogElement] = {
    addStandardDBOutputParameters(operatorDialog, defaultOutputName)
  }

  /**
    * Adds parameters to your operator to allow the user to determine how the output of the
    * database operator will be written.
    * This method adds the following parameters.
    *   -- "viewOrTable": a radio button which lets the user choose whether to store the output
    *      as a view or a table
    *   -- "outputSchema": a drop down with all the defined schemas in the database, so that the
    *      user can pick their output schema.
    *   -- "dropIfExists" : Inherited from the super class. A radio button so the user can select
    *      to drop the output table/view if it already exists or throw an exception.
    *   -- "outputName" : The name of the output table.
    *      The default is the userID+the workflow name + the operator UUID
    *
    * @param operatorDialog From the 'onExecution' method's parameters. This method
    *                       adds the parameters to this object.
    * @param defaultOutputName Optional. If you would like the default output to be something other
    *                          than "tmpTable"
    * @return A sequence of each of the parameters that were created and added to the operatorDialog
    */
  def addStandardDBOutputParameters(operatorDialog: OperatorDialog,
                                    defaultOutputName: String = operatorNameUUIDVariable
                                   ): Seq[DialogElement] = {
    val outputType = addViewOrTableRadioButton(operatorDialog)
    val schema = addDBSchemaDropDownBox(operatorDialog)
    val tableName = addResultTableNameParameter(operatorDialog, defaultTableName, "Output Table")
    val dropIfExists = addDropIfExistsParameter(operatorDialog)
    Seq(schema, outputType, dropIfExists, tableName)
  }

  /**
   * Use in the runtime class:
   * Gets the value of "outputSchema" parameter
   *
   * @param parameters OperatorParameters instance [containing the "outputSchema" parameter].
   * @throws java.lang.NullPointerException if the parameter wasn't added to the parameters object
   * @return the output schema name
   */
  @throws(classOf[NullPointerException])
  def getDBOutputSchemaParam(parameters: OperatorParameters): String = {
    parameters.getStringValue(outputSchemaParameterId)
  }

}
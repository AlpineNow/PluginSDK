/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog.{DialogElement, OperatorDialog}

/**
 * This is a utility class used to handle standard parameters
 * for Hive operators.
 */
object HiveParameterUtils extends TableOutputParameterUtils {

  val resultDBNameParameterID = "hiveResultDBName"

  /**
   * Adds the standard output parameters for a single Hive table output.
   * In particular:
   *  -- "hiveResultTableName": The table name for output.
   *  -- "hiveResultDBName": The database name for output (may be empty).
   *  -- "viewOrTable" : a radio button which lets the user choose whether to store the output
   *      as a view or a table
   *  -- "overwrite": A Boolean parameter asking if the user wants to overwrite old output.
   *
   * @param operatorDialog OperatorDialog box to add the parameters to.
   * @return A sequence of the dialog elements added.
   */
  def addStandardOutputParameters(operatorDialog: OperatorDialog,
                                  defaultDatabaseName: String = defaultDatabaseName,
                                  defaultTableName: String = defaultTableName
                                   ): Seq[DialogElement] = {
    val databaseName = addResultDatabaseNameParameter(operatorDialog, defaultDatabaseName)
    val tableName = addResultTableNameParameter(operatorDialog, defaultTableName)
    val viewOrTable = addViewOrTableRadioButton(operatorDialog)
    val overwrite = addOverwriteParameter(operatorDialog)
    Seq(databaseName, viewOrTable, tableName, overwrite)
  }

  def addResultDatabaseNameParameter(operatorDialog: OperatorDialog,
                                     defaultDatabaseName: String = defaultDatabaseName): DialogElement = {
    val databaseName = operatorDialog.addStringBox(resultDBNameParameterID, "Result Database Name", defaultDatabaseName, ".*", 0, 0)
    databaseName
  }

  /**
   * Gets the value of the database name parameter as an option.
   * Will return None if the parameter is missing or empty
   * (in which case the code should not specify the database
   * and the default database will be used).
   * @param parameters OperatorParameters instance [containing the database name parameter].
   * @return Option wrapping the database name parameter.
   */
  def getResultDBName(parameters: OperatorParameters): Option[String] = {
    if (parameters.contains(resultDBNameParameterID)) {
      val strValue = parameters.getStringValue(resultDBNameParameterID)
      if (strValue == null || strValue.isEmpty) {
        None
      } else {
        Some(strValue)
      }
    } else {
      None
    }
  }

}

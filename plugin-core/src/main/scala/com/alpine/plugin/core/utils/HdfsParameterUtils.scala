/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog._

/**
 * Utility for the standard parameters for use by operators which use
 * HDFS datasets.
 */
object HdfsParameterUtils extends OutputParameterUtils {

  val outputDirectoryParameterID = "outputDirectory"
  val outputNameParameterID = "outputName"

  /**
   * Adds
   * -- "outputDirectory": an HDFS directory selector for the location of the output
   * -- "outputName": a StringBox parameter for the name of the output.
   * -- "overwrite": A Boolean parameter asking if the user wants to overwrite old output.
   *
   * These are the standard parameters to be used when an operator outputs a HDFS dataset.
   *
   * The default value of the output name will be @operator_name_uuid,
   * which will be replaced at runtime with the actual operator name and uuid
   * concatenated with a underscore, sanitized to make it a valid file name.
   *
   * @param defaultOutputName The default value for the output name parameter.
   * @return A sequence of the dialog elements added.
   */
  def addStandardHdfsOutputParameters(operatorDialog: OperatorDialog,
                                      defaultOutputName: String = operatorNameUUIDVariable): Seq[DialogElement] = {
    val outputDirectorySelector = addOutputDirectorySelector(operatorDialog)
    val outputName = addOutputNameParameter(operatorDialog, defaultOutputName)
    val overwrite = addOverwriteParameter(operatorDialog)
    Seq(outputDirectorySelector, outputName, overwrite)
  }

  /**
   * adds a string dialog box to let the user define the name of the file with the output.
   * @param operatorDialog The dialog to which the parameter will be added.
   * @param defaultOutputName The default value to be used for the parameter.
   * @return
   */
  def addOutputNameParameter(operatorDialog: OperatorDialog,
                             defaultOutputName: String): DialogElement = {
    val outputName = operatorDialog
      .addStringBox(
        outputNameParameterID,
        "Output Name",
        defaultOutputName,
        ".+", 0, 0
      )
    outputName
  }

  /**
   * Adds directory selector box to let the user select
   * the location in HDFS where the results of the operator will be written
   * @param operatorDialog The dialog to which the parameter will be added.
   * @return
   */
  def addOutputDirectorySelector(operatorDialog: OperatorDialog): DialogElement = {
    val outputDirectorySelector = operatorDialog.addHdfsDirectorySelector(
      outputDirectoryParameterID,
      "Output Directory",
      "@default_tempdir/alpine_out/@user_name/@flow_name"
    )
    outputDirectorySelector
  }

  /**
   * Returns the String parameter value corresponding to id "outputDirectory", or empty String
   * if that parameter is not present.
   * @return String parameter value of "outputDirectory" parameter if present, otherwise empty String.
   */
  private def getOutputDirectory(parameters: OperatorParameters): String = {
    val outputDirectory: String = parameters.getStringValue(outputDirectoryParameterID)
    if (outputDirectory == null) {
      ""
    } else {
      outputDirectory
    }
  }

  /**
   * Returns the String parameter value corresponding to id "outputName", or empty String
   * if that parameter is not present.
   * @return String parameter value of "outputName" parameter if present, otherwise empty String.
   */
  private def getOutputName(parameters: OperatorParameters): String = {
    val outputName: String = parameters.getStringValue(outputNameParameterID)
    if (outputName == null) {
      ""
    } else {
      outputName
    }
  }

  /**
   * Concatenates the string values of parameters with keys "outputDirectory" and "outputName"
   * with a file separator.
   * @return The output path corresponding to parameters outputDirectory and outputName.
   */
  def getOutputPath(parameters: OperatorParameters): String = {
    getOutputDirectory(parameters) + '/' + getOutputName(parameters)
  }

}

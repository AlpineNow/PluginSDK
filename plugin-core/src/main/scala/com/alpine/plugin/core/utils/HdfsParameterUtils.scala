/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog._
import com.alpine.plugin.core.io.TabularFormatAttributes
import scala.util.{Failure, Success, Try}

/**
 * Utility for the standard parameters for use by operators which use
 * HDFS datasets.
 */
object HdfsParameterUtils extends OutputParameterUtils {

  val outputDirectoryParameterID = "outputDirectory"
  val outputNameParameterID = "outputName"
  val storageFormatParameterID = "storageFormat"

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
   * Adds a dropdown menu to select the storage format for a tabular dataset output.
   * I.e., it'll add a dropdown menu with available selections 'TSV', 'Parquet' and 'Avro'.
   * @param operatorDialog The operator dialog where you are going to add the dropdown menu.
   * @param defaultFormat The default format one wants to use.
   * @return The dropdown dialog element.
   */
  def addHdfsStorageFormatParameter(operatorDialog: OperatorDialog,
                                    defaultFormat: HdfsStorageFormat.HdfsStorageFormat = HdfsStorageFormat.TSV): DialogElement = {
    val formats = HdfsStorageFormat.values.map(_.toString)
    operatorDialog.addDropdownBox(
      storageFormatParameterID,
      "Storage format",
      formats.toSeq,
      defaultFormat.toString
    )
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

  /**
   * Get the Hdfs storage format from the parameters object.
   * @param parameters This must contain the format parameter. I.e., the user should've
   *                   called addHdfsStorageFormatParameter before.
   * @return The selected Hdfs storage format.
   */
  def getHdfsStorageFormat(parameters: OperatorParameters): HdfsStorageFormat.HdfsStorageFormat = {
    val parameterValue = parameters.getStringValue(storageFormatParameterID)
    if (parameterValue == null) {
      HdfsStorageFormat.TSV // Defaults to TSV if this parameter is missing.
    } else {
      Try(HdfsStorageFormat.withName(parameterValue)) match {
        case Success(f) => f
        case Failure(_) => HdfsStorageFormat.TSV // Defaults to TSV if the parameter value is strange.
      }
    }
  }

  /**
   * Get default tabular format attributes to use (e.g., delimiter, quote information for CSV/TSV).
   * This is useful if one wants to define output formats using default values.
   * @param storageFormat The Hdfs storage format.
   * @return Tabular format attributes.
   */
  def getTabularFormatAttributes(storageFormat: HdfsStorageFormat.HdfsStorageFormat): TabularFormatAttributes = {
    storageFormat match {
      case HdfsStorageFormat.Parquet => TabularFormatAttributes.createParquetFormat()
      case HdfsStorageFormat.Avro => TabularFormatAttributes.createAvroFormat()
      case HdfsStorageFormat.TSV => TabularFormatAttributes.createTSVFormat()
    }
  }
}

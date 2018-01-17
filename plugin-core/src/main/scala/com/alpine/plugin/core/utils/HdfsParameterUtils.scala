/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */
package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog._
import scala.util.{Failure, Success, Try}

/**
  * Utility for the standard parameters for use by operators which use
  * HDFS datasets.
  */
object HdfsParameterUtils extends OutputParameterUtils {

  val outputDirectoryParameterID = "outputDirectory"
  val outputNameParameterID = "outputName"
  val storageFormatParameterID = "storageFormat"
  val compressionTypeParameterID = "compressionType"

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
    * Adds
    * -- "storageFormat": an Dropdown Box for storage format.
    * -- "compressionType": a Dropdown Box for compression type.
    *
    * Note: The compression type selected must be supported by the storage format chosen, otherwise the CO will stay invalid at design time.
    *
    * @param operatorDialog The default value for the output name parameter.
    * @param defaultStorageFormat The default format one wants to use.
    * @param defaultCompression The default compression one wants to use.
    * @return A sequence of the dialog elements added.
    */
  def addHdfsStorageAndCompressionParameters(operatorDialog: OperatorDialog,
      defaultStorageFormat: HdfsStorageFormatType,
      defaultCompression: HdfsCompressionType): Seq[DialogElement] = {
    val storageParameter = addHdfsStorageFormatParameter(operatorDialog, defaultStorageFormat)
    val compressionParameter = addHdfsCompressionParameter(operatorDialog, defaultCompression)
    Seq(storageParameter, compressionParameter)
  }

  /***
    * Same method as `addHdfsStorageAndCompressionParameters` defined above, with:
    * - default storage format as Parquet
    * - default compression as Gzip
    */
  def addHdfsStorageAndCompressionParameters(operatorDialog: OperatorDialog): Seq[DialogElement] = {
    addHdfsStorageAndCompressionParameters(operatorDialog, HdfsStorageFormatType.Parquet, HdfsCompressionType.Gzip)
  }

  /**
    * Adds a dropdown menu to select the storage format for a tabular dataset output.
    * I.e., it'll add a dropdown menu with available selections 'CSV', 'Parquet' and 'Avro'.
    *
    * @param operatorDialog The operator dialog where you are going to add the dropdown menu.
    * @param defaultFormat  The default format one wants to use.
    * @return The dropdown dialog element.
    */
  def addHdfsStorageFormatParameter(operatorDialog: OperatorDialog,
                                    defaultFormat: HdfsStorageFormatType): DialogElement = {
    val formats = HdfsStorageFormatType.values.map(_.toString)
    operatorDialog.addDropdownBox(
      storageFormatParameterID,
      "Storage Format",
      formats,
      defaultFormat.toString
    )
  }


  /**
    * Adds a dropdown menu to select the compression Type for a tabular dataset output.
    * I.e., it'll add a dropdown menu with available selections 'No Compression', 'Snappy', 'GZIP' and 'Deflate'.
    *
    * @param operatorDialog The operator dialog where you are going to add the dropdown menu.
    * @param defaultCompression  The default compression one wants to use.
    * @return The dropdown dialog element.
    */
  def addHdfsCompressionParameter(operatorDialog: OperatorDialog,
                                  defaultCompression: HdfsCompressionType): DropdownBox = {
    val formats = HdfsCompressionType.values.map(_.toString)
    operatorDialog.addDropdownBox(
      compressionTypeParameterID,
      "Compression",
      formats,
      defaultCompression.toString
    )
  }


  /**
    * adds a string dialog box to let the user define the name of the file with the output.
    *
    * @param operatorDialog    The dialog to which the parameter will be added.
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
        ".+", required = true)
    outputName
  }

  /**
    * Adds directory selector box to let the user select
    * the location in HDFS where the results of the operator will be written
    *
    * @param operatorDialog The dialog to which the parameter will be added.
    * @return
    */
  def addOutputDirectorySelector(operatorDialog: OperatorDialog): DialogElement = {
    val outputDirectorySelector = operatorDialog.addOutputDirectorySelector(
      outputDirectoryParameterID,
      "Output Directory",
      isRequired = true
    )
    outputDirectorySelector
  }


  /**
    * Returns the String parameter value corresponding to id "outputDirectory", or empty String
    * if that parameter is not present.
    *
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
    *
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
    *
    * @return The output path corresponding to parameters outputDirectory and outputName.
    */
  def getOutputPath(parameters: OperatorParameters): String = {
    getOutputDirectory(parameters) + '/' + getOutputName(parameters)
  }

  /**
    * Get the Hdfs storage format from the parameters object.
    *
    * @param parameters This must contain the format parameter. I.e., the user should've
    *                   called addHdfsStorageFormatParameter or addHdfsStorageAndCompressionParameters  before.
    * @return The selected Hdfs storage format.
    */
  def getHdfsStorageFormatType(parameters: OperatorParameters): HdfsStorageFormatType = {
    val parameterValue = parameters.getStringValue(storageFormatParameterID)
    if (parameterValue == null) {
      HdfsStorageFormatType.CSV // Defaults to CSV if this parameter is missing.
    } else {
      Try(HdfsStorageFormatType.withName(parameterValue)) match {
        case Success(f) => f
        case Failure(_) => HdfsStorageFormatType.CSV // Defaults to CSV if the parameter value is strange.
      }
    }
  }

  /**
    * Get the Hdfs compression type from the parameters object.
    *
    * @param parameters This must contain the compression parameter. I.e., the user should've
    *                   called addHdfsCompressionParameter or addHdfsStorageAndCompressionParameters before.
    * @return The selected Hdfs compression type.
    */
  def getHdfsCompressionType(parameters: OperatorParameters): HdfsCompressionType = {
    val parameterValue = parameters.getStringValue(compressionTypeParameterID)
    if (parameterValue == null) {
      HdfsCompressionType.NoCompression //Defaults to none if parameter is missing (and for compatibility with pre-6.4 operators)
    } else {
      Try(HdfsCompressionType.withName(parameterValue)) match {
        case Success(f) => f
        case Failure(_) => HdfsCompressionType.NoCompression // Defaults to None if the parameter value is strange.
      }
    }
  }

  //bad data Reporting:
  val badDataReportParameterID: String = "badData"

  @deprecated()
  val DEFAULT_NUMBER_ROWS: Int = 1000
  @deprecated()
  val badDataReportNROWS: String = "Write Up to " + DEFAULT_NUMBER_ROWS + " Null Rows to File"

  val badDataLocation: String = "_BadData"

  def addNullDataReportParameter(operatorDialog: OperatorDialog, message: String = "Write Rows Removed Due to Null Data to File"): DialogElement = {
    val badDataSelector = operatorDialog.addDropdownBox(badDataReportParameterID,
      "Write Rows Removed Due to Null Data To File", NullDataReportingStrategy.displayOptions, NullDataReportingStrategy.doNotWriteDisplay)
    badDataSelector
  }

  def getNullDataReportParameter(parameters: OperatorParameters): String = {
    convertParameterValueFromLegacy(parameters.getStringValue(badDataReportParameterID))
  }

  @deprecated("Use getNullDataStrategy")
  def getAmountOfBadDataToWrite(parameters: OperatorParameters): Option[Long] = {
    val paramValue = getNullDataReportParameter(parameters)
    if (paramValue.equals(NullDataReportingStrategy.doNotWriteDisplay)) {
      None
    } else if (paramValue.equals(badDataReportNROWS)) {
      Some(Long.MaxValue)
    } else if (paramValue.equals(NullDataReportingStrategy.writeAndCountDisplay)) {
      Some(Long.MaxValue)
    } else {
      None
    }
  }

  def getNullDataStrategy(parameters: OperatorParameters,
                          alternatePath : Option[String]): NullDataReportingStrategy = {
    val paramValue = getNullDataReportParameter(parameters)
    if (paramValue.equals(NullDataReportingStrategy.doNotWriteDisplay)) {
      NullDataReportingStrategy.DoNotWrite
    } else if (paramValue.equals(NullDataReportingStrategy.writeAndCountDisplay)) {
      val path = alternatePath match {
      case Some(s) => s
      case None => HdfsParameterUtils.getBadDataPath(parameters)
      }
      NullDataReportingStrategy.WriteAndCount(path)
    } else {
      NullDataReportingStrategy.DoNotCount
    }
  }

  private def convertParameterValueFromLegacy(oldValue: String): String = {
    if (NullDataReportingStrategy.displayOptions.contains(oldValue)) {
      oldValue
    } else
      oldValue match {
        case ("No") => NullDataReportingStrategy.doNotWriteDisplay
        case ("Yes") => NullDataReportingStrategy.writeAndCountDisplay
        case ("Partial (1000) Rows") => NullDataReportingStrategy.writeAndCountDisplay
        case ("No and Do Not Count Rows Removed (Fastest)") => NullDataReportingStrategy.noCountDisplay
        case (_) =>
          println(" Warning could not find reference for parameter value " + oldValue)
           NullDataReportingStrategy.doNotWriteDisplay
      }
  }

  def countRowsRemovedDueToNullData(parameters: OperatorParameters): Boolean = {
    val paramValue = getNullDataReportParameter(parameters)
    !paramValue.equals(NullDataReportingStrategy.noCountDisplay)
  }


  def getBadDataPath(parameters: OperatorParameters): String = {
    val outputPath = HdfsParameterUtils.getOutputPath(parameters)
    outputPath + badDataLocation
  }

}

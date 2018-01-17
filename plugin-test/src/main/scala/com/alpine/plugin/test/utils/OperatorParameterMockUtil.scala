/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.test.utils

import java.util

import com.alpine.plugin.core.dialog.IRowDialogRow
import com.alpine.plugin.core.utils._
import com.alpine.plugin.test.mock.OperatorParametersMock


object OperatorParameterMockUtil {

  val defaultOutputDirectory = "target/testResults"

  /**
   * Use to se the value of a TabularColumnSelectionBox parameter.
   */
  def addTabularColumns(params: OperatorParametersMock, paramId: String, colNames: String*): Unit = {
    val map: util.HashMap[String, util.ArrayList[String]] = new util.HashMap[String, util.ArrayList[String]]()
    val arrayList = new util.ArrayList[String]()
    colNames.foreach(colName => arrayList.add(arrayList.size(), colName))
    map.put("group", arrayList)
    params.setValue(paramId, map)
  }

  /**
   * Use to set the value of a "TabularColumnDropDown" parameter.
   */
  def addTabularColumn(params: OperatorParametersMock, paramId: String, colName: String): Unit = {
    val map: util.HashMap[String, String] = new util.HashMap[String, String]()
    map.put("group", colName)
    params.setValue(paramId, map)
  }

  /**
    * Use to set the value of a "RowDialogSetup" parameter.
    */
  def addRowDialogElements(params: OperatorParametersMock, paramId: String, rows: IRowDialogRow*): Unit = {
    val arrayList = new util.ArrayList[IRowDialogRow]()
    rows.foreach(row => arrayList.add(arrayList.size(), row))
    params.setValue(paramId,arrayList)
  }

  /**
    * Use to add all the standard HDFS parameters to the mock parameters object
    */
  def addHdfsParams(operatorParametersMock: OperatorParametersMock, outputName: String,
                    outputDirectory: String, storageFormat: HdfsStorageFormatType,
                    overwrite: Boolean, compressionType: HdfsCompressionType = HdfsCompressionType.NoCompression): OperatorParametersMock = {
    operatorParametersMock.setValue(HdfsParameterUtils.outputDirectoryParameterID, outputDirectory)
    operatorParametersMock.setValue(HdfsParameterUtils.outputNameParameterID, outputName)
    operatorParametersMock.setValue(HdfsParameterUtils.storageFormatParameterID, storageFormat)
    operatorParametersMock.setValue(HdfsParameterUtils.compressionTypeParameterID, compressionType)
    operatorParametersMock.setValue(HdfsParameterUtils.overwriteParameterID, overwrite.toString)
    operatorParametersMock
  }

  /**
    * Uses the following default values
    * outputDirectory = "target/testResults"
    * storageFormat type = HdfsStorageFormatType.CSV
    * overwrite = true
    */
  def addHdfsParamsDefault(operatorParametersMock: OperatorParametersMock, outputName: String): OperatorParametersMock = {
    addHdfsParams(operatorParametersMock, outputName, defaultOutputDirectory, HdfsStorageFormatType.CSV, overwrite = true, compressionType = HdfsCompressionType.NoCompression)
  }

  def makeArrayList(selections: String*): util.ArrayList[String] = {
    val arrayList = new util.ArrayList[String]()
    selections.foreach(arrayList.add)
    arrayList
  }

  def addCheckBoxSelections(operatorParametersMock: OperatorParametersMock, paramId : String, values : String*): Unit ={
    operatorParametersMock.setValue(paramId, makeArrayList(values : _ * ))
  }
}

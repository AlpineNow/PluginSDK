package com.alpine.plugin.test.utils

import java.util

import com.alpine.plugin.core.utils.HdfsStorageFormat.HdfsStorageFormat
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HdfsStorageFormat}
import com.alpine.plugin.test.mock.OperatorParametersMock



object ParameterMockUtil {
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
   * Use to add all the standard HDFS parameters to the mock parameters object
   */
  def addHdfsParams(operatorParametersMock: OperatorParametersMock, outputName: String,
                    outputDirectory: String = defaultOutputDirectory, storageFormat: HdfsStorageFormat
                    = HdfsStorageFormat.TSV,
                    overwrite: Boolean = true): OperatorParametersMock = {
    operatorParametersMock.setValue(HdfsParameterUtils.outputDirectoryParameterID, outputDirectory)
    operatorParametersMock.setValue(HdfsParameterUtils.outputNameParameterID, outputName)
    operatorParametersMock.setValue(HdfsParameterUtils.storageFormatParameterID, storageFormat)
    operatorParametersMock.setValue(HdfsParameterUtils.overwriteParameterID, overwrite.toString)
    operatorParametersMock
  }
}

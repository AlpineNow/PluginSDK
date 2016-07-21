package com.alpine.plugin.test.utils

import com.alpine.plugin.core.OperatorGUINode
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.test.mock._
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite


class SimpleAbstractSparkJobSuite extends FunSuite {
  import TestSparkContexts._
  lazy val sparkUtils = {
    new SparkRuntimeUtils(sc)
  }

  /**
   * Writes the contents of the dataFrame to a file and creates and "HdfsTabularDatasetObject"
   * corresponding to that data. Use this method to convert from a dataFrame,
   * to the HdfsTabularDataset
   * type which Custom Operators take as input.
   * This method deletes what is written at the path before writing to it.
   */
  def createHdfsTabularDatasetLocal(dataFrame: DataFrame, opInfo: Option[OperatorInfo],
                                    path: String) = {
    sparkUtils.deleteFilePathIfExists(path)
    sparkUtils.saveAsCSV(path, dataFrame, TSVAttributes.defaultCSV, opInfo)
  }

  /**
   * Test the transform with addendum method of an operator written using the DataFrame template.
   */
  def runDataFrameThroughDFTemplate(dataFrame: DataFrame, operator: SparkDataFrameJob, params: OperatorParametersMock) = {
    operator.transformWithAddendum(params, dataFrame, sparkUtils, new SimpleOperatorListener)
  }

  /**
   * Use to test the saving and reading of a Operator with HdfsTabularDataset as input and Output.
   * Will work for plugins written using DataFrame template.
   */
  def runDataFrameThroughOperator(dataFrame: DataFrame,
                                  operator: SparkIOTypedPluginJob[HdfsTabularDataset, HdfsTabularDataset],
                                  appConf: collection.mutable.Map[String, String] = collection.mutable.Map.empty,
                                  parameters: OperatorParametersMock): DataFrame = {

    val hdfsTabularDataset = createHdfsTabularDatasetLocal(dataFrame, Some(parameters.operatorInfo()),
      OperatorParameterMockUtil.defaultOutputDirectory)
    val result = operator.onExecution(sc, appConf, hdfsTabularDataset, parameters, new SimpleOperatorListener)
    sparkUtils.getDataFrame(result)
  }

  /**
   * Use to test Custom Operators that use SparkIOTypedPluginJob, but do not use the DataFrame
   * template. This method runs the onExecution method with using the giving input, operator, and
   * parameters.
   * Default appConf is an empty map.
   */
  def runInputThroughOperator[I <: IOBase, O <: IOBase](input: I,
                                                        operator: SparkIOTypedPluginJob[I, O], parameters: OperatorParametersMock,
                                                        appConf: collection.mutable.Map[String, String] =
                                                        collection.mutable.Map.empty) = {
    operator.onExecution(sc, appConf, input, parameters, new SimpleOperatorListener)
  }

  def runInputThroughEntireOperator[I <: IOBase, O <: IOBase](input: I, operatorGUINode: OperatorGUINode[I, O],
                                                              operatorSparkJob: SparkIOTypedPluginJob[I, O],
                                                              inputParameteres: OperatorParametersMock,
                                                              tabularSchema: Option[TabularSchema],
                                                              appConf: collection.mutable.Map[String, String] =
                                                              collection.mutable.Map.empty) = {
    val newParameters = getNewParametersFromGUI(input, operatorGUINode, inputParameteres, tabularSchema)
     operatorSparkJob.onExecution(sc, appConf, input, newParameters, new SimpleOperatorListener)
  }

  def getNewParametersFromGUI[I <: IOBase, O <: IOBase](input: I, operatorGUINode: OperatorGUINode[I, O],
                                                        inputParameters: OperatorParametersMock,
                                                        tabularSchema: Option[TabularSchema],
                                                        dataSourceName: String = "dataSource") = {
    val operatorDialogMock = new OperatorDialogMock(inputParameters, input, tabularSchema)
    operatorGUINode.onPlacement(operatorDialogMock,
      OperatorDataSourceManagerMock(dataSourceName),
      new OperatorSchemaManagerMock())
    operatorDialogMock.getNewParameters
  }

  def getNewParametersFromDataFrameGui[T <: SparkDataFrameJob](testGUI: SparkDataFrameGUINode[T],
                                                               inputHdfs: HdfsTabularDataset,
                                                               inputParameters: OperatorParametersMock,
                                                               dataSourceName: String = "dataSource") = {
    val operatorDialogMock = new OperatorDialogMock(inputParameters,
      inputHdfs, Some(inputHdfs.tabularSchema))
    testGUI.onPlacement(operatorDialogMock,
      OperatorDataSourceManagerMock(dataSourceName),
      new OperatorSchemaManagerMock())
    operatorDialogMock.getNewParameters
  }

  def runDataFrameThroughEntireDFTemplate[T <: SparkDataFrameJob](operatorGUI: SparkDataFrameGUINode[T],
                                                                  operatorJob: T,
                                                                  inputParams: OperatorParametersMock,
    dataFrameInput: DataFrame, path: String = "target/testResults") = {
    val inputHdfs = HdfsDelimitedTabularDatasetDefault(
      path, sparkUtils.convertSparkSQLSchemaToTabularSchema(dataFrameInput.schema), TSVAttributes.defaultCSV, None)
    val parameters = getNewParametersFromDataFrameGui(operatorGUI, inputHdfs, inputParams)
    runDataFrameThroughDFTemplate(dataFrameInput, operatorJob, parameters)
  }
}


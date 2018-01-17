package com.alpine.plugin.test.utils

import com.alpine.plugin.core.OperatorGUINode
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.{AlpineSparkEnvironment, SparkIOTypedPluginJob}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.test.mock._
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite


class SimpleAbstractSparkJobSuite extends FunSuite {

  import TestSparkContexts._

  lazy val sparkUtils: SparkRuntimeUtils = {
    new SparkRuntimeUtils(sparkSession)
  }

  def alpineSparkEnvironment(appConf : scala.collection.mutable.Map[String, String]): AlpineSparkEnvironment = {
    new AlpineSparkEnvironment(sparkSession, appConf)
  }


  /**
    * Writes the contents of the dataFrame to a file and creates and "HdfsTabularDatasetObject"
    * corresponding to that data. Use this method to convert from a dataFrame,
    * to the HdfsTabularDataset
    * type which Custom Operators take as input.
    * This method deletes what is written at the path before writing to it.
    */
  def createHdfsTabularDatasetLocal(dataFrame: DataFrame, path: String): HdfsDelimitedTabularDatasetDefault = {
    sparkUtils.deleteFilePathIfExists(path)
    sparkUtils.saveAsCSV(path, dataFrame, TSVAttributes.defaultCSV)
  }

  /**
    * Test the transform with addendum method of an operator written using the DataFrame template.
    */
  def runDataFrameThroughDFTemplate(dataFrame: DataFrame,
                                    operator: SparkDataFrameJob,
                                    params: OperatorParametersMock): (DataFrame, Map[String, AnyRef]) = {
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
    val sparkEnv = alpineSparkEnvironment(appConf)
    val hdfsTabularDataset = createHdfsTabularDatasetLocal(dataFrame,
      OperatorParameterMockUtil.defaultOutputDirectory)
    val result = operator.onExecution(sparkEnv, hdfsTabularDataset, parameters, new SimpleOperatorListener)
    sparkUtils.getDataFrame(result)
  }

  /**
    * Use to test Custom Operators that use SparkIOTypedPluginJob, but do not use the DataFrame
    * template. This method runs the onExecution method with using the giving input, operator, and
    * parameters.
    * Default appConf is an empty map.
    */
  def runInputThroughOperator[I <: IOBase, O <: IOBase](
      input: I,
      operator: SparkIOTypedPluginJob[I, O],
      parameters: OperatorParametersMock,
      appConf: collection.mutable.Map[String, String] = collection.mutable.Map.empty
  ): O = {
    val sparkEnv = alpineSparkEnvironment(appConf)
    operator.onExecution(sparkEnv, input, parameters, new SimpleOperatorListener)
  }


  def runInputThroughEntireOperator[I <: IOBase, O <: IOBase](
      input: I,
      operatorGUINode: OperatorGUINode[I, O],
      operatorSparkJob: SparkIOTypedPluginJob[I, O],
      inputParameters: OperatorParametersMock,
      appConf: collection.mutable.Map[String, String] = collection.mutable.Map.empty
  ): O = {
    val sparkEnv = alpineSparkEnvironment(appConf)
    val newParameters = getNewParametersFromGUI(input, operatorGUINode, inputParameters)
    operatorSparkJob.onExecution(sparkEnv, input, newParameters, new SimpleOperatorListener)
  }


  def getNewParametersFromGUI[I <: IOBase, O <: IOBase](input: I,
                                                        operatorGUINode: OperatorGUINode[I, O],
                                                        inputParameters: OperatorParametersMock,
                                                        dataSourceName: String = "dataSource"): OperatorParametersMock = {
    val operatorDialogMock = OperatorDialogMock(inputParameters, input)
    operatorGUINode.onPlacement(operatorDialogMock,
      OperatorDataSourceManagerMock(dataSourceName),
      new OperatorSchemaManagerMock())
    operatorDialogMock.getNewParameters
  }

  def getNewParametersFromDataFrameGui[T <: SparkDataFrameJob](testGUI: SparkDataFrameGUINode[T],
                                                               inputHdfs: HdfsTabularDataset,
                                                               inputParameters: OperatorParametersMock,
                                                               dataSourceName: String = "dataSource"): OperatorParametersMock = {
    val operatorDialogMock = new OperatorDialogMock(inputParameters, inputHdfs)
    testGUI.onPlacement(operatorDialogMock,
      OperatorDataSourceManagerMock(dataSourceName),
      new OperatorSchemaManagerMock())
    operatorDialogMock.getNewParameters
  }

  def runDataFrameThroughEntireDFTemplate[T <: SparkDataFrameJob](operatorGUI: SparkDataFrameGUINode[T],
                                                                  operatorJob: T,
                                                                  inputParams: OperatorParametersMock,
                                                                  dataFrameInput: DataFrame,
                                                                  path: String = "target/testResults"): (DataFrame, Map[String, AnyRef]) = {
    val inputHdfs = HdfsDelimitedTabularDatasetDefault(
      path, sparkUtils.convertSparkSQLSchemaToTabularSchema(dataFrameInput.schema), TSVAttributes.defaultCSV)
    val parameters = getNewParametersFromDataFrameGui(operatorGUI, inputHdfs, inputParams)
    runDataFrameThroughDFTemplate(dataFrameInput, operatorJob, parameters)
  }
}


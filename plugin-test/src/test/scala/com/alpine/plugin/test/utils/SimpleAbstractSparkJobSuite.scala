package com.alpine.plugin.test.utils

import com.alpine.plugin.core.io.{IOBase, HdfsTabularDataset, OperatorInfo}
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.templates.SparkDataFrameJob
import com.alpine.plugin.core.spark.utils.{TestSparkContexts, SparkRuntimeUtils}
import com.alpine.plugin.test.mock.{SimpleOperatorListener, OperatorParametersMock}
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
    sparkUtils.saveAsTSV(path, dataFrame, opInfo)
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
      ParameterMockUtil.defaultOutputDirectory)
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
}
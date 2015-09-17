package com.alpine.plugin.test

import java.util

import com.alpine.plugin.core.io.{OperatorInfo, HdfsTabularDataset}
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.test.mock.OperatorParametersMock
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class AbstractSparkJobSuite extends EnvironmentSetup {

  lazy val sparkUtils = {
    new SparkRuntimeUtils(sc)
  }

  def createHdfsTabularDataset(dataFrame: DataFrame, opInfo: Option[OperatorInfo]) = {
    val path = cluster.createDirectory("/tmp").getAbsolutePath
    sparkUtils.saveAsTSV(path + "/data", dataFrame, opInfo)
  }

  def makeAValue(value: String) = {
    val hashMap = new util.HashMap[java.lang.String, util.ArrayList[java.lang.String]]()
    val list = new util.ArrayList[java.lang.String]()
    list.add(value)
    hashMap.put("nice code base", list)
    hashMap
  }

  def getOperatorParameters(params: (String, String)*) = {
    val parameters = new OperatorParametersMock("operatorName", "operatorUUID")
    params.foreach({ case (key, value) => parameters.setValue(key, makeAValue(value))})
    parameters
  }


  def runDataFrameThroughOperator(dataFrame: DataFrame,
                                  operator: SparkIOTypedPluginJob[HdfsTabularDataset, HdfsTabularDataset],
                                  appConf: mutable.Map[String, String] = mutable.Map.empty,
                                  parameters: OperatorParametersMock = getOperatorParameters()): DataFrame = {
    val result = operator.onExecution(sc, appConf, createHdfsTabularDataset(dataFrame, Some(parameters.operatorInfo())), parameters, new SimpleOperatorListener)
    sparkUtils.getDataFrame(result)
  }

}

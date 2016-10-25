/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.spark

import com.alpine.plugin.core.utils.SparkParameterUtils

import scala.collection.mutable

/**
 * Spark job configuration.
 */


case class SparkJobConfiguration(
  userDefinedParameters : mutable.Map[String, String], autoTuneMissingValues : Boolean,
  autoTunerOptions: AutoTunerOptions
){
 //Required for backward compatibility
  def additionalParameters :mutable.Map[String, String]=  userDefinedParameters
  def executorMemoryMB : Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkExecutorMBElementId,"-1").toInt
  def numExecutors: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkNumExecutorsElementId, "-1").toInt
  def driverMemoryMB: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkDriverMBElementId, "-1").toInt
  def numExecutorCores: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkNumExecutorCoresElementId, "-1").toInt
}


object SparkJobConfiguration{
  @deprecated("use constructor that takes only that map ")
  def apply(numExecutors: Int,
            executorMemoryMB: Int,
            driverMemoryMB: Int,
            numExecutorCores: Int,
            additionalParameters: mutable.Map[String, String] =
            mutable.Map[String, String]()) : SparkJobConfiguration = {
    additionalParameters ++= Map(
      SparkParameterUtils.sparkNumExecutorsElementId -> numExecutors.toString,
      SparkParameterUtils.sparkNumExecutorCoresElementId -> numExecutorCores.toString,
      SparkParameterUtils.sparkExecutorMBElementId -> executorMemoryMB.toString,
      SparkParameterUtils.sparkDriverMBElementId -> driverMemoryMB.toString)
    new SparkJobConfiguration(additionalParameters, autoTuneMissingValues = true,
      AutoTunerOptions(driverMemoryFraction = 1.0, fileSizeMultiplier = 4.0))
  }

}

case class AutoTunerOptions(options : mutable.Map[String, String]){
  def addOption(key : String, value : String ): this.type ={
    options += (key-> value)
    this
  }

  def getOrElse(key : String, default : Double) : Double = {
    try{
      options(key).toDouble
    }catch{
      case (e : Throwable) => default
    }
  }

}

object AutoTunerOptions{

  val driverMemoryFractionId = "driverMemoryFraction"
  val fileSizeMultiplierId = "fileSizeMultiplier"

  /**
    * Create a simple version of the AutoTuner options.
    * This includes two basic constants used in the auto tuning process.
    * @param driverMemoryFraction if the data is large we set the executors to the size of a yarn
    *                             container. By default we set the driver to the same size as the executors.
    *                             However, if your computation does not return much
    *                             data to the driver you may not need so much driver memory. This scalar
    *                             provides a way to scale the size of the driver. If you have a highly
    *                             parallelizable algorithm that does not return much input to the driver
    *                             (say KMeans) try setting this to less then one.
    *                              If you want the driver memory to be larger
    *                             even on small input data (perhaps for an algorithm that aggregates
    *                             by a group and returns data to the driver try setting it to more than one.
    *
    * @param fileSizeMultiplier  In auto tuning we try to use enough executors so that the input data
    *                            could be read in memory into the compute layer of the executors.
    *                             We calculate the size of the input data
    *                            in memory by multiplying the file size (which you can see by right
    *                            clicking a dataset in alpine and viewing the "Hadoop File Properties"
    *                            dialog) by a scalar.
    *
    *                            The default if 4. i.e. we are assuming that you are using all of the input data
    *                            and that it is perhaps a dataset of integers which take roughly four
    *                            times as much space on memory than on disk. Adjust this value based
    *                            on your estimation of the resources required. For example, if your
    *                            operation immediately filters the data to a few integer columns a better
    *                            multiplier might be (selectedColumns)/(totalColumns) * 4.
    *
    */
  def apply(driverMemoryFraction : Double, fileSizeMultiplier : Double): AutoTunerOptions ={
    AutoTunerOptions(mutable.Map(driverMemoryFractionId -> driverMemoryFraction.toString,
      fileSizeMultiplierId -> fileSizeMultiplier.toString))
  }
}

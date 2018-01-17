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
                                  userDefinedParameters: mutable.Map[String, String], autoTuneMissingValues: Boolean,
                                  autoTunerOptions: AutoTunerOptions
                                ) {
  //Required for backward compatibility
  def additionalParameters: mutable.Map[String, String] = userDefinedParameters

  def executorMemoryMB: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkExecutorMBElementId, "-1").toInt

  def numExecutors: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkNumExecutorsElementId, "-1").toInt

  def driverMemoryMB: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkDriverMBElementId, "-1").toInt

  def numExecutorCores: Int =
    userDefinedParameters.getOrElse(SparkParameterUtils.sparkNumExecutorCoresElementId, "-1").toInt
}


object SparkJobConfiguration {
  @deprecated("use constructor that takes only that map ")
  def apply(numExecutors: Int,
            executorMemoryMB: Int,
            driverMemoryMB: Int,
            numExecutorCores: Int,
            additionalParameters: mutable.Map[String, String] =
            mutable.Map[String, String]()): SparkJobConfiguration = {
    additionalParameters ++= Map(
      SparkParameterUtils.sparkNumExecutorsElementId -> numExecutors.toString,
      SparkParameterUtils.sparkNumExecutorCoresElementId -> numExecutorCores.toString,
      SparkParameterUtils.sparkExecutorMBElementId -> executorMemoryMB.toString,
      SparkParameterUtils.sparkDriverMBElementId -> driverMemoryMB.toString)
    new SparkJobConfiguration(additionalParameters, autoTuneMissingValues = true,
      AutoTunerOptions(driverMemoryFraction = 1.0, inputCachedSizeMultiplier = 1.0, 0L))
  }

}

case class AutoTunerOptions(options: mutable.Map[String, String]) {
  def addOption(key: String, value: String): this.type = {
    options += (key -> value)
    this
  }

  def getOrElse(key: String, default: Double): Double = {
    try {
      options(key).toDouble
    } catch {
      case (e: Throwable) => default
    }
  }

  def getOrElse(key : String, default : Long) : Long = {
    try{
      options(key).toLong
    } catch {
      case (e : Throwable) => default
    }
  }

}

object AutoTunerOptions {

  val driverMemoryFractionId = "driverMemoryFraction"
  val inputCachedSizeMultiplierId = "inputCachedSizeMultiplier"
  val minExecutorMemoryId = "minExecutorMemoryAlgorithm"

  /**
    * Create a simple version of the AutoTuner options.
    * This includes two basic constants used in the auto tuning process.
    *
    * @param driverMemoryFraction   if the data is large we set the executors to the size of a yarn
    *                               container. By default we set the driver to the same size as the executors.
    *                               However, if your computation does not return much
    *                               data to the driver you may not need so much driver memory. This scalar
    *                               provides a way to scale the size of the driver. If you have a highly
    *                               parallelizable algorithm that does not return much input to the driver
    *                               (say KMeans) try setting this to less then one.
    *                               If you want the driver memory to be larger
    *                               even on small input data (perhaps for an algorithm that aggregates
    *                               by a group and returns data to the driver try setting it to more than one.
    * @param inputCachedSizeMultiplier  In auto tuning we try to use enough executors so that the input data
    *                                   could be read in memory into the compute layer of the executors.
    *                                   We calculate the size of the input data
    *                                   in memory by multiplying the file size (which you can see by right
    *                                   clicking a dataset in alpine and viewing the "Hadoop File Properties"
    *                                   dialog) by a scalar Y = X * inputCachedSizeMultiplier, where X is a coefficient
    *                                   which takes into account the storage format, compression, and input column data types.
    *                                   For multiple tabular inputs we use the sum of the estimated input data memory for all inputs.
    *                                   Default value for inputCachedSizeMultiplier is 1.0, adjust this value based on your
    *                                   estimation of the resources required and if the operation is expensive.
    * @param minExecutorMemory  the minimum size of the executor memory. This is design to insure that
    *                           for algorithms that perform very expensive computations on the executors,
    *                           that the executor memory is set to be pretty large, even if the cluster
    *                           or input data is relatively small.
    */
  def apply(
    driverMemoryFraction: Double,
    inputCachedSizeMultiplier: Double,
    minExecutorMemory : Long): AutoTunerOptions = {

    val inputCachedSizeMultiplierUpdated = if (inputCachedSizeMultiplier <= 0.0) 1.0 else inputCachedSizeMultiplier

    AutoTunerOptions(mutable.Map(driverMemoryFractionId -> driverMemoryFraction.toString,
      inputCachedSizeMultiplierId -> inputCachedSizeMultiplierUpdated.toString,
      minExecutorMemoryId -> minExecutorMemory.toString))
  }

  /**
    * Same as above but sets min executor memory to zero. In this case it will be overridden by the
    * alpine conf.
    * @return
    */
  def apply(driverMemoryFraction : Double, inputCachedSizeMultiplier : Double): AutoTunerOptions = {
    apply(driverMemoryFraction, inputCachedSizeMultiplier, 0L)
  }

}
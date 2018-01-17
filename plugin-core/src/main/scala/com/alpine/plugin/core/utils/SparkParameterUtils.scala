/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog.{OperatorDialog, SparkParameter}

import scala.util.Try

/**
  * Convenience functions for directly adding Spark related options to the
  * dialog window.
  */
object SparkParameterUtils {

  val sparkNumExecutorsElementId = "spark_numExecutors"
  val sparkExecutorMBElementId = "spark_executorMB"
  val sparkDriverMBElementId = "spark_driverMB"
  val sparkNumExecutorCoresElementId = "spark_numExecutorCores"
  val storageLevelParamId = "spark_storage_level"
  val disableDynamicAllocationParamId = "noDynamicAllocation"
  val numPartitionsId = "numPartitions"
  val repartitionRDDId = "repartitionRDD"

  def addStandardSparkOptions(operatorDialog: OperatorDialog, additionalSparkParameters: List[SparkParameter]): Unit = {
    val list = List(
      SparkParameter(disableDynamicAllocationParamId,
        "Disable Dynamic Allocation", false.toString, userSpecified = false, overridden = false),
      SparkParameter(sparkNumExecutorsElementId, "Number of Executors", "-1", userSpecified = false, overridden = false),
      SparkParameter(sparkExecutorMBElementId, "Executor Memory in MB", "-1", userSpecified = false, overridden = false),
      SparkParameter(sparkDriverMBElementId, "Driver Memory in MB", "-1", userSpecified = false, overridden = false),
      SparkParameter(sparkNumExecutorCoresElementId, "Number of Executor Cores", "-1", userSpecified = false, overridden = false)
    )
    operatorDialog.addAdvancedSparkSettingsBox("sparkSettings", "Advanced Spark Settings",
      list ++ additionalSparkParameters)
  }

  @deprecated("The default values are no longer used due to the auto tuning. If you " +
    "would like to maintain the old behavior use 'operatorDialog.addAdvancedSparkSettingsBox' directly" +
    "otherwise use signature without integer params")
  def addStandardSparkOptions(
    operatorDialog: OperatorDialog,
    defaultNumExecutors: Int,
    defaultExecutorMemoryMB: Int,
    defaultDriverMemoryMB: Int,
    defaultNumExecutorCores: Int, additionalSparkParameters: List[SparkParameter]) {

    addStandardSparkOptions(operatorDialog, additionalSparkParameters)
  }

  @deprecated("The default values are no longer used due to the auto tuning. If you " +
    "would like to maintain the old behavior use 'operatorDialog.addAdvancedSparkSettingsBox' directly" +
    "otherwise use signature without integer params")
  def addStandardSparkOptions(
    operatorDialog: OperatorDialog,
    defaultNumExecutors: Int,
    defaultExecutorMemoryMB: Int,
    defaultDriverMemoryMB: Int,
    defaultNumExecutorCores: Int) {

    addStandardSparkOptions(operatorDialog, List[SparkParameter]())
  }

  /**
    * A more advanced method for adding SparkP Parameters.
    * Will also add a "StorageLevel" Parameter which will indicate what level of persistence to use
    * within a Spark job.
    * NOTE: The storage level parameter cannot be set automatically during runtime.
    * To have any effect the custom operator developer must implement RDD persistence with this value
    * (retrievable with 'getStorageLevel' method) in the Spark Job class of their operator.
    *
    * @param defaultStorageLevel       - default storage level e.g. NONE or "MEMORY_AND_DISK.
    * @param additionalSparkParameters - a list of a additional Spark Parameters.
    */
  def addStandardSparkOptionsWithStorageLevel(operatorDialog: OperatorDialog,
    defaultNumExecutors: Int,
    defaultExecutorMemoryMB: Int,
    defaultDriverMemoryMB: Int,
    defaultNumExecutorCores: Int,
    defaultStorageLevel: String,
    additionalSparkParameters: List[SparkParameter] =
    List.empty[SparkParameter]) {
    val list = List(
      makeStorageLevelParam(defaultStorageLevel))
    addStandardSparkOptions(operatorDialog, list ++ additionalSparkParameters)
  }

  // =====================================================================================================
  //  Additional Spark Parameters:
  //  Add these parameters to the additionalSparkParameters list when using the "addStandardSparkOptions"
  //  method. To create Specific Spark parameters with the same UI as those in the Alpine Core
  // =====================================================================================================

  /**
    * Add storage level param. Default must be the string representation of a Spark Storage Level
    * e.g. "MEMORY_AND_DISK"
    *
    * @param defaultStorageLevel
    * @return
    */
  def makeStorageLevelParam(defaultStorageLevel: String): SparkParameter =
    SparkParameter(storageLevelParamId, "Storage Level", defaultStorageLevel, userSpecified = false, overridden = false)

  /**
    * Create a "Repartition Data" checkbox to let the user determine whether the input data should
    * be shuffled to increase the number of partitions.
    */
  def makeRepartitionParam: SparkParameter =
    SparkParameter(repartitionRDDId, "Repartition Data", "", userSpecified = false, overridden = false)

  /**
    * Create a "Number of Partitions" parameter to let the user determine how many partitions should
    * be used either when repartitioning the data (controlled by above param) or in when shuffling
    * generally.
    * If this parameter is not set, a value will be selected by auto tuning. If a value is selected
    * this value will be used to set the "spark.default.parallelism" (or "spark.sql.shuffle.partitions" for Spark SQL)
    * parameters which controls the default number of parameter used in a wide transformation in Spark.
    *
    * @return
    */
  def makeNumPartitionsParam: SparkParameter =
    SparkParameter(numPartitionsId, "Number of Partitions", "", userSpecified = false, overridden = false)


  /**
    * Retrieve storage level param added via "makeStorageLevelParam" from advanced parameters box.
    * Return NONE if the parameter was not added.
    * NOTE: this method does not validate the String, so if the users put in an invalid storage
    * level parameter, calling StorageLevel.fromString(s) on the result of this method will fail.
    */
  def getStorageLevel(operatorParameters: OperatorParameters): Option[String] = {
    operatorParameters.getAdvancedSparkParameters.get(storageLevelParamId)
  }

  /**
    * Return true if the repartition parameter was added AND the user checked it. Return false
    * otherwise. Note that because this parameter is exposed as a check box by the alpine engine,
    * the value of the parameter will be either "true" or "false" (string representation of java
    * booleans).
    **/

  def getRepartition(operatorParameters: OperatorParameters): Boolean = {
    operatorParameters.getAdvancedSparkParameters.getOrElse(repartitionRDDId, "false") == "true"
  }

  /**
    * Retrieve the value of the number of partitions parameter added to the advanced spark box.
    * However, the CO developer should be aware that Alpine Auto Tuning determines an optimal number
    * of partitions to use and sets that value to spark.default.parallelism. So before repartitioning
    * the developer should check that that is set if this method returns None.
    */
  def getUserSetNumPartitions(operatorParameters: OperatorParameters): Option[Int] = {
    Try(operatorParameters.getAdvancedSparkParameters(storageLevelParamId).toInt).toOption
  }
}

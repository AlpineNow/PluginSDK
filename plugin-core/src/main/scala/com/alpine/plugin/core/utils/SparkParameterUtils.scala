/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.dialog.SparkParameter

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

  def addStandardSparkOptions(operatorDialog: OperatorDialog, additionalSparkParameters: List[SparkParameter]): Unit ={
    val list = List(
      new SparkParameter(disableDynamicAllocationParamId,
        "Disable Dynamic Allocation", false.toString, false, false),
      new SparkParameter(sparkNumExecutorsElementId, "Number of Executors", "3", false, false),
      new SparkParameter(sparkExecutorMBElementId, "Executor Memory in MB", "-1", false, false),
      new SparkParameter(sparkDriverMBElementId, "Driver Memory in MB", "-1", false, false),
      new SparkParameter(sparkNumExecutorCoresElementId, "Number of Executor Cores", "-1", false, false)
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
    * @param defaultStorageLevel - default storage level e.g. NONE or "MEMORY_AND_DISK.
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
      new SparkParameter(storageLevelParamId, "Storage Level", defaultStorageLevel, false, false))
    addStandardSparkOptions(operatorDialog,list ++ additionalSparkParameters)
  }

  def getStorageLevel(operatorParameters: OperatorParameters): Option[String] = {
    operatorParameters.getAdvancedSparkParameters.get(storageLevelParamId)
  }
}

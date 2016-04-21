/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.utils

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
  def addStandardSparkOptions(
    operatorDialog: OperatorDialog,
    defaultNumExecutors: Int,
    defaultExecutorMemoryMB: Int,
    defaultDriverMemoryMB: Int,
    defaultNumExecutorCores: Int
  ) {

    val list = List(
      new SparkParameter(sparkNumExecutorsElementId, "Number of Executors", defaultNumExecutors.toString, false, false),
      new SparkParameter(sparkExecutorMBElementId, "Executor Memory in MB", defaultExecutorMemoryMB.toString, false, false),
      new SparkParameter(sparkDriverMBElementId, "Driver Memory in MB", defaultDriverMemoryMB.toString, false, false),
      new SparkParameter(sparkNumExecutorCoresElementId, "Number of Executor Cores", defaultNumExecutorCores.toString, false, false)
    )
    operatorDialog.addAdvancedSparkSettingsBox("sparkSettings", "Advanced Spark Settings", list)
  }
}

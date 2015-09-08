/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.utils

import com.alpine.plugin.core.dialog.OperatorDialog

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
    defaultNumExecutorCores: Int) {
    operatorDialog.addIntegerBox(
      sparkNumExecutorsElementId,
      "Number of Executors",
      1,
      Int.MaxValue,
      defaultNumExecutors
    )

    operatorDialog.addIntegerBox(
      sparkExecutorMBElementId,
      "Executor Memory in MB",
      1,
      Int.MaxValue,
      defaultExecutorMemoryMB
    )
    
    operatorDialog.addIntegerBox(
      sparkDriverMBElementId,
      "Driver Memory in MB",
      1,
      Int.MaxValue,
      defaultDriverMemoryMB
    )
    
    operatorDialog.addIntegerBox(
      sparkNumExecutorCoresElementId,
      "Number of Executor Cores",
      1,
      Int.MaxValue,
      defaultNumExecutorCores
    )
  }
}

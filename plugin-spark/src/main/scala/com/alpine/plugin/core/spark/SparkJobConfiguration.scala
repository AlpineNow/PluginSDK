/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.spark

import scala.collection.mutable

/**
 * Spark job configuration.
 */
case class SparkJobConfiguration(
  numExecutors: Int,
  executorMemoryMB: Int,
  driverMemoryMB: Int,
  numExecutorCores: Int,
  additionalParameters: mutable.Map[String, String] = mutable.Map[String, String]()
)

package com.alpine.plugin.util

import com.alpine.plugin.core.config.CustomOperatorConfig

import scala.util.Try

/** *
  * Utils to analyze Java memory usage when running a job on the Alpine Server (locally).
  * This should be leveraged in the Runtime class of the operator to avoid memory issues.
  * Note: The `java_max_memory_usage` parameter value can be configured by an admin in the "custom_operator" section of the alpine.conf.
  * If not specified or not in ]0,100[, a default value of 90 is used.
  */
object JavaMemoryUsage {
  val DEFAULT_MAX_MEM_USAGE = 90.0
  val co_config_max_mem_usage = "java_max_memory_usage"
  private val bytesPerMB = 1024 * 1024

  private def bytesToMBString(bytes: Long) = {
    "%.2f".format(bytes.toDouble / JavaMemoryUsage.bytesPerMB) + "MB"
  }
}

class JavaMemoryUsage(config: CustomOperatorConfig) {
  // All in bytes.
  private var max = 0L
  private var total = 0L
  private var free = 0L
  private var used = 0L
  // From 0 to 100.
  private var percentageUsed = .0
  refreshUsages()

  def refreshUsages(): Unit = {
    val runtime = Runtime.getRuntime
    free = runtime.freeMemory
    max = runtime.maxMemory
    total = runtime.totalMemory
    // https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory
    used = total - free
    percentageUsed = 100 * used / max
  }

  def getMemoryUsagePercentage: Double = percentageUsed

  def getUsedMemory: Double = used

  def getMaxMemory: Double = max

  lazy val maxMemoryUsageFromConfig: Double = {
    val configEntry: Try[Double] = Try(config.entries(JavaMemoryUsage.co_config_max_mem_usage).toString.toDouble)
    if (configEntry.isSuccess && configEntry.get > 0 && configEntry.get < 100) configEntry.get
    else JavaMemoryUsage.DEFAULT_MAX_MEM_USAGE
  }

  def isMemoryUsageUnderLimit: Boolean = {
    refreshUsages()
    if (!(percentageUsed < maxMemoryUsageFromConfig)) {
      Runtime.getRuntime.gc
      refreshUsages()
    }
    percentageUsed < maxMemoryUsageFromConfig
  }

  override def toString: String = {
    s"JavaMemoryUsage{ usage: $percentageUsed%, free: ${JavaMemoryUsage.bytesToMBString(free)}, used: ${JavaMemoryUsage.bytesToMBString(used)}, max: ${JavaMemoryUsage.bytesToMBString(max)}, total: ${JavaMemoryUsage.bytesToMBString(total)}}"
  }
}
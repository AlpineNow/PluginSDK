package com.alpine.plugin.core.spark

import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import org.apache.spark.sql.SparkSession

/**
  * This object is made available to the user during the spark job and has the relevant context of the
  * spark job, including the spark session, the application conf and access to Alpine's runtime environment
  */
 class AlpineSparkEnvironment(val sparkSession: SparkSession, val applicationConf : scala.collection.mutable.Map[String, String]) {
   def getSparkUtils = new SparkRuntimeUtils(sparkSession)
}


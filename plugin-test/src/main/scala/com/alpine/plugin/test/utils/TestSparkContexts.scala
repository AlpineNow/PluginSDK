package com.alpine.plugin.test.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object TestSparkContexts {

  lazy val sparkSession: SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    SparkSession.builder().appName("udf testings")
        .master("local")
        .config("spark.ui.enabled", value = false)
        .getOrCreate()
  }

  lazy val sc: SparkContext = sparkSession.sparkContext

  @deprecated("use spark session directly")
  lazy val sqlContext: SQLContext =sparkSession.sqlContext

}
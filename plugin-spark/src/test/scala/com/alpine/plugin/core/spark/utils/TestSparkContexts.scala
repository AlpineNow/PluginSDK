package com.alpine.plugin.core.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object TestSparkContexts {
  lazy val sc: SparkContext = new SparkContext("local", "test")
  lazy val sqlContext: SQLContext = new SQLContext(sc)
}
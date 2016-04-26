package com.alpine.plugin.test.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object TestSparkContexts {

  lazy val sc: SparkContext = {
    // http://stackoverflow.com/questions/27781187/how-to-stop-messages-displaying-on-spark-console
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local", "test")
  }
  lazy val sqlContext: SQLContext = new SQLContext(sc)
}
package com.alpine.plugin.core.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterAll

trait SparkContextTest extends BeforeAndAfterAll {
  self: org.scalatest.Suite =>
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll() {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    sc = new SparkContext("local", "test")
    sqlContext = new SQLContext(sc)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.driver.host")
    super.afterAll()
  }
}
package com.alpine.plugin.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class EnvironmentSetup extends FunSuite with BeforeAndAfterAll {

  var cluster: MiniCluster = null
  var sc: SparkContext = null
  var sContext: SQLContext = null

  override def beforeAll() {
    
    cluster = new MiniCluster
    cluster.start()

    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.test", "")
      .set("spark.sql.hive.metastore.barrierPrefixes", "org.apache.spark.sql.hive.execution.PairSerDe")
    sc = new SparkContext(conf)
    sContext = new SQLContext(sc)
  }

  override def afterAll() {
    cluster.shutdown()
    sc.stop
  }

}

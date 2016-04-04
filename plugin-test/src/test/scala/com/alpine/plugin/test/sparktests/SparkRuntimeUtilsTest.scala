package com.alpine.plugin.test.sparktests

import com.alpine.plugin.core.io.TSVAttributes
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.HdfsStorageFormat
import com.alpine.plugin.test.utils.TestSparkContexts
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite


class SparkRuntimeUtilsTest extends FunSuite {

  import TestSparkContexts._

  val path = "plugin-test/src/test/resources/TestData.csv"
  val fishPath = "target/test-results/FishData"
  val carsSchema = new StructType(
    Array(
      StructField("year", IntegerType, nullable = true),
      StructField("make", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true)
    ))

  val sparkUtils = new SparkRuntimeUtils(sc)

  test("Check read dirty data") {
    val f = HdfsDelimitedTabularDatasetDefault(path,
      sparkUtils.convertSparkSQLSchemaToTabularSchema(carsSchema), TSVAttributes.defaultCSV, None)
    val results = sparkUtils.getDataFrame(f)
    val resultRows = results.collect()
    assert(resultRows.toSet.equals(Set(
      Row.fromTuple(2014, null, "Volt", 5000.0),
      Row.fromTuple(2015, null, "Volt", 5000.0))))
    assert(resultRows.length == 2)
  }

  test("Write with nullValue as empty string, and delim as pipe") {

    val pipeAttributes = TSVAttributes(
      delimiter = '|',
      escapeStr = TSVAttributes.DEFAULT_ESCAPE_CHAR,
      quoteStr = TSVAttributes.DEFAULT_QUOTE_CHAR,
      containsHeader = false,
      nullString = "")
    val originalData = Seq(
      FishColor("red", "fish"),
      FishColor("blue", "fish"),
      FishColor("", "fish"))

    val dataFrame = sqlContext.createDataFrame(sc.parallelize(originalData))

    val fishDataOutput = sparkUtils.saveDataFrame(fishPath, dataFrame, HdfsStorageFormat.TSV,
      overwrite = true, None, Map[String, AnyRef](), pipeAttributes)
    val readData = sparkUtils.getDataFrame(fishDataOutput).collect()
    val nulls = readData.filter(row => row.anyNull)

    assert(readData.length == 3)
    assert(nulls.length == 1)

    val asTextFile = sc.textFile(fishPath)
    assert(asTextFile.first().split('|').length == 2)
  }

  test("Test Storage Utils") {

    val h: HdfsStorageFormat = HdfsStorageFormat.Avro
    val m = h match {
      case HdfsStorageFormat.Avro => 1
      case HdfsStorageFormat.Parquet => 2
      case HdfsStorageFormat.TSV => 3
    }

    assert(m == 1)
  }

}

case class FishColor(color: String, fish: String) extends Serializable
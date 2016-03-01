package com.alpine.plugin.test.sparktests

import com.alpine.plugin.core.spark.utils.BadDataReportingUtils
import com.alpine.plugin.test.utils.TestSparkContexts
import org.apache.spark.sql
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FunSuite

import scala.util.Try


class BadDataReportingUtilsTest extends FunSuite {
  import TestSparkContexts._

  val inputRows = List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23))
  val badData: List[sql.Row] = List(Row(null, 1), Row("Ulia2", null), Row("Nastya2", null),
    Row("Olga", null))
  val inputSchema =
    StructType(List(StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)))
  val outputPath = "target/testResults"

  test("Test reporting bad data as String RDD ") {
    val rdd = sc.parallelize(List("", "some"))
    val sqlContext = new SQLContext(rdd.sparkContext)
    val dummySchema = StructType(Array(StructField("String", StringType, nullable = true)))
    val badDataAsDF = sqlContext.createDataFrame(rdd.map(r => Row.fromSeq(Seq(r))), dummySchema)
    val (data, msg) = BadDataReportingUtils.getBadDataToWriteAndMessage(Some(3), outputPath,
      6, 3, Some(badDataAsDF))
    assert(data.get.count() == 2)
    assert(msg.contains("<br> All bad data written to file: <br>"))
  }

  test("Reporting bad data as DataFrame ") {
    val badDF = sqlContext.createDataFrame(sc.parallelize(badData), inputSchema)
    val writeBadDataParam: Option[Long] = Some(Int.MaxValue)
    val badDataPath = outputPath + "/test2"
    val (data, report) = BadDataReportingUtils.getBadDataToWriteAndMessage(writeBadDataParam, badDataPath,
      6, 3, Some(badDF))
    val resultData = data.get.collect()
    assert(resultData.length == badData.length)
  }

  test("test method to filter bad data from data frame ") {
    val goodInputData = sqlContext.createDataFrame(sc.parallelize(inputRows), inputSchema)
    val badInputData = sqlContext.createDataFrame(sc.parallelize(badData), inputSchema)
    val allData = goodInputData.unionAll(badInputData)
    val (goodDataOutput, badDataOutput) = BadDataReportingUtils.removeDataFromDataFrame(
      row => row.anyNull, allData,
      Some(2))

    val badDataRows = badDataOutput.get.collect().toSet
    val goodDataRows = goodDataOutput.collect().toSet
    assert(badDataRows.equals(badData.toSet))
    assert(goodDataRows.equals(inputRows.toSet))
  }

  test("test the remove zeros and nulls function used in the bad data reporting plugin ") {
    val badData2: List[sql.Row] = List(Row("Sofya", 0), Row("Natasha", null),
      Row("Olga", null))
    val goodInputData = sqlContext.createDataFrame(sc.parallelize(inputRows), inputSchema)
    val badInputData = sqlContext.createDataFrame(sc.parallelize(badData2), inputSchema)
    val allData = goodInputData.unionAll(badInputData)
    val (goodDataOutput, badDataOutput) = BadDataReportingUtils.removeDataFromDataFrame(
      RowProcessingUtil.containsZeros,
      allData, Some(2))
    val badDataRows = badDataOutput.get.collect().toSet
    val goodDataRows = goodDataOutput.collect().toSet
    assert(badDataRows.equals(badData2.toSet))
    assert(goodDataRows.equals(inputRows.toSet))
  }
}

object RowProcessingUtil extends Serializable {
  def containsZeros(r: Row): Boolean = {
    if (r.anyNull) true
    else {
      val m = r.toSeq.map(v => Try(v.toString.toDouble))
      m.exists(
        wrappedValue => wrappedValue.isSuccess &&
          (wrappedValue.get == 0.0))
    }
  }
}


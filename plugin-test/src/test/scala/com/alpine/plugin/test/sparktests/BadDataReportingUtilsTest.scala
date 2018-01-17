package com.alpine.plugin.test.sparktests

import com.alpine.plugin.core.spark.utils.BadDataReportingUtils
import com.alpine.plugin.core.utils.{HdfsParameterUtils, NullDataReportingStrategy}
import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.sql
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.io.File
import scala.util.Try


class BadDataReportingUtilsTest extends SimpleAbstractSparkJobSuite {

  val badDataReportALL = NullDataReportingStrategy.writeAndCountDisplay
  val badDataReportNO_COUNT = NullDataReportingStrategy.noCountDisplay

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

    val dummySchema = StructType(Array(StructField("String", StringType, nullable = true)))
    val badDataAsDF = sparkSession.createDataFrame(rdd.map(r => Row.fromSeq(Seq(r))), dummySchema)
    val (data, msg) = BadDataReportingUtils.getBadDataToWriteAndMessage(Some(3), outputPath,
      6, 3, Some(badDataAsDF))
    assert(data.get.count() == 2)
    assert(msg.contains("All the data removed (due to null values) has been written to file:"))
  }

  test("Reporting bad data as DataFrame ") {
    val badDF = sparkSession.createDataFrame(sc.parallelize(badData), inputSchema)
    val writeBadDataParam: Option[Long] = Some(Int.MaxValue)
    val badDataPath = outputPath + "/test2"
    val (data, report) = BadDataReportingUtils.getBadDataToWriteAndMessage(writeBadDataParam, badDataPath,
      6, 3, Some(badDF))
    val resultData = data.get.collect()
    assert(resultData.length == badData.length)
  }

  test("Reporting bad data as DataFrame with new method ") {
    val badDF = sparkSession.createDataFrame(sc.parallelize(badData), inputSchema)
    val writeBadDataParam: Option[Long] = Some(Int.MaxValue)
    val badDataPath = outputPath + "/test2"
    val (data, report) = BadDataReportingUtils.getNullDataToWriteMessage(writeBadDataParam, badDataPath,
      6, 3, Some(badDF), "Rows removed due to null data")
    val resultData = data.get.collect()
    assert(resultData.length == badData.length)
  }

  test("test method to filter bad data from data frame ") {
    val goodInputData = sparkSession.createDataFrame(sc.parallelize(inputRows), inputSchema)
    val badInputData = sparkSession.createDataFrame(sc.parallelize(badData), inputSchema)
    val allData = goodInputData.union(badInputData)
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
    val goodInputData = sparkSession.createDataFrame(sc.parallelize(inputRows), inputSchema)
    val badInputData = sparkSession.createDataFrame(sc.parallelize(badData2), inputSchema)
    val allData = goodInputData.union(badInputData)
    val (goodDataOutput, badDataOutput) = BadDataReportingUtils.removeDataFromDataFrame(
      RowProcessingUtil.containsZeros,
      allData, Some(2))
    val badDataRows = badDataOutput.get.collect().toSet
    val goodDataRows = goodDataOutput.collect().toSet
    assert(badDataRows.equals(badData2.toSet))
    assert(goodDataRows.equals(inputRows.toSet))
  }

  test("NullDataAndReportGeneralMethod") {
    val goodInputData = sparkSession.createDataFrame(sc.parallelize(inputRows ++ badData), inputSchema)

    val (badDataFile, df, message) = testBadDataReporting(badDataReportALL, goodInputData)
    assert(badDataFile.isDirectory)
    assert(message.contains("Input size after removing rows because it is evil: </td><td style = \"padding-right:10px;\" >3 rows"))
  }

  test("Bad data with nothing in it ") {
    val goodInputData = sparkSession.createDataFrame(sc.parallelize(inputRows), inputSchema)
    val (badDataFile, badDataDf, message) = testBadDataReporting(badDataReportALL, goodInputData)
    assert(!badDataFile.isDirectory)
    assert(message.contains("No data removed because it is evil"))
    assert(!message.contains("null"))
    assert(message == "<table ><tr><td style = \"padding-right:10px;\" >" +
      "Input data size: </td><td style = \"padding-right:10px;\" >3 rows</td></tr><tr><td style = \"padding-right:10px;\" >" +
      "Input size after removing rows because it is evil: </td><td style = \"padding-right:10px;\" >3 rows</td></tr><tr><td style = \"padding-right:10px;\" >" +
      "No data removed because it is evil</td><td style = \"padding-right:10px;\" ></td></tr></table>")
  }

  test("Test Null Data Reporting NOT counting the rows ") {
    val inputData = sparkSession.createDataFrame(sc.parallelize(inputRows ++ badData), inputSchema)

    val (badDataFile, df, msg) = testBadDataReporting(badDataReportNO_COUNT, inputData)
    testNotRemovingTheRows(badDataFile, df, msg)
  }


  test("Bad Data Reporting with old parameter values  ") {
    val inputData = sparkSession.createDataFrame(sc.parallelize(inputRows ++ badData), inputSchema)

    val badDataReportNO_COUNT = {
      //Testing in the not counting and not writing to file case
      val (badDataFile, df, msg) = testBadDataReporting("No and Do Not Count Rows Removed (Fastest)", inputData)
      testNotRemovingTheRows(badDataFile, df, msg)
    }

    val badDataReport_NO = {
      val (badDataFile, df, msg) = testBadDataReporting("No", inputData)
      assert(!badDataFile.isDirectory)
      assert(df.count() == inputRows.length)
      assert(msg.contains("Input size after removing rows because it is evil: </td><td style = \"padding-right:10px;\" >3 rows"))
    }

    val badDataReport_YES = {
      val (badDataFile, df, msg) = testBadDataReporting("Yes", inputData)
      assert(badDataFile.isDirectory)
      assert(df.count() == inputRows.length)
      assert(msg.contains("Input size after removing rows because it is evil: </td><td style = \"padding-right:10px;\" >3 rows"))
      badDataFile.deleteRecursively()
    }

    val badDataReport_PARTIAL = {
      val (badDataFile, df, msg) = testBadDataReporting("Partial (" + HdfsParameterUtils.DEFAULT_NUMBER_ROWS + ") Rows", inputData)
      assert(badDataFile.isDirectory)
      assert(df.count() == inputRows.length)
      assert(msg.contains("Input size after removing rows because it is evil: </td><td style = \"padding-right:10px;\" >3 rows"))
      badDataFile.deleteRecursively()
    }

  }

  def testBadDataReporting(badDataOption: String, inputData: DataFrame): (File, DataFrame, String) = {
    val mockParams = new OperatorParametersMock("Thing", "One")
    OperatorParameterMockUtil.addHdfsParamsDefault(mockParams, "BadDataTest")
    mockParams.setValue(HdfsParameterUtils.badDataReportParameterID, badDataOption)

    val f = new File(new java.io.File(HdfsParameterUtils.getBadDataPath(mockParams)))
    if (f.isDirectory) {
      f.deleteRecursively()
    }
    val (goodData, message) = BadDataReportingUtils.filterNullDataAndReportGeneral(_.anyNull,
      inputData, mockParams, sparkUtils, "because it is evil")

    val badDataFile = new File(new java.io.File(HdfsParameterUtils.getBadDataPath(mockParams)))
    (badDataFile, goodData, message)
  }


  def testNotRemovingTheRows(badDataFile: File, badDataDF: DataFrame, msg: String): Unit = {
    assert(badDataDF.count() == inputRows.length)
    assert(!badDataFile.isDirectory)
    assert(msg.contains("You have selected not to count the number" +
      " of rows removed to speed up the computation of the operator."))
    assert(!msg.contains("null"))
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


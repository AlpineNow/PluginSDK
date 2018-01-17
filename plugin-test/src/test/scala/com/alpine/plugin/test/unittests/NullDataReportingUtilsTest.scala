package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.spark.reporting.NullDataReportingUtils
import com.alpine.plugin.core.utils._
import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class NullDataReportingUtilsTest extends SimpleAbstractSparkJobSuite{
  import  com.alpine.plugin.core.utils.NullDataReportingStrategy._
  import com.alpine.plugin.test.utils.TestSparkContexts._
  var dataFrame : Option[DataFrame] = None

  test("Count but do not write  data frame template "){

    val (output, summary) = testNullDataOption(doNotWriteDisplay,
      "outlook", "nullData1A")
    assert(output.count() == 17)

    //When there are no null values in the data frame
    val (output2, summary2) = testNullDataOption(doNotWriteDisplay,
      "play", "nullData1B")
    assert(output2.count() == 18) //Nothing should have been removed
  }

  test("Do not count "){
    val (output, summary) = testNullDataOption(noCountDisplay,
      "outlook", "nullData2A")
    assert(output.count() == 17)

    //When there are no null values in the data frame
    val (output2, summary2) = testNullDataOption(noCountDisplay,
      "play", "nullData2B")
    assert(output2.count() == 18) //Nothing should have been removed
  }

  test("Write null data  "){
    val (output, summary) = testNullDataOption(writeAndCountDisplay,
      "outlook", "nullData3A")
    assert(output.count() == 17)
    val p = "target/testResults/nulldata/nullData3A_BadData"
    val text  =  sc.textFile(p).collect()
    assert(text.length == 1)
    assert(summary.contains("<table ><tr><td style = \"padding-right:10px;\" >Input data size: </td>" +
                            "<td style = \"padding-right:10px;\" >18 rows</td></tr><tr><td style =" +
                            " \"padding-right:10px;\" >Input size after removing rows because prediction column is null:" +
                            " </td><td style = \"padding-right:10px;\" >17 rows</td></tr><tr><td style = " +
                            "\"padding-right:10px;\" >Rows removed because prediction column is null: </td><td style =" +
                            " \"padding-right:10px;\" >1 rows (6%)</td></tr></table><br>All the data removed " +
                            "(because prediction column is null) has been written to file: " +
                            "<br>target/testResults/nulldata/nullData3A_BadData<br>"))

    //When there are no null values in the data frame
    val (output2, summary2) = testNullDataOption(writeAndCountDisplay,
      "play", "nullData3B")
    assert(output2.count() == 18) //Nothing should have been removed
     val p2 = "target/testResults/nulldata/nullData3B_BadData"
    assert(Try(sc.textFile(p2).collect()).isFailure) //Path does not exist
  }

  test(" test count "){
    val allBadDf = GolfData.createGolfDF(sparkSession)
    val condition = allBadDf.col("play").notEqual("5")
    val (good, bad) = NullDataReportingUtils.countBadData(allBadDf, condition)
    val fullFunction = NullDataReportingUtils.filterNullDataAndReportGeneral(allBadDf, sparkUtils, NullDataReportingStrategy.DoNotWrite, condition, "")
    val collect = fullFunction._1.collect()
    assert(collect.length == 0)
  }


  def testNullDataOption(option : String, testColumn : String, path : String): (DataFrame, String) ={
    val golfWithNulls = dataFrame.getOrElse(GolfData.createGolfDFWithNullAndZeroRows(sparkSession))
    val operatorGUI = new TestGui()
    val operatorJob = new TestSparkJob()
    val params = new OperatorParametersMock("1", "2")
    OperatorParameterMockUtil.addHdfsParams(params, path, "target/testResults/nulldata", HdfsStorageFormatType.CSV, true)
    OperatorParameterMockUtil.addTabularColumns(params, "addTabularDatasetColumnCheckboxes",
      golfWithNulls.columns.filter(!_.equals(testColumn)) :_*)
    OperatorParameterMockUtil.addTabularColumn(params, "addTabularDatasetColumnDropdownBox", testColumn)
    params.setValue(HdfsParameterUtils.badDataReportParameterID, option)
    val (output, addendum) =  this.runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, params, golfWithNulls, path)
    val summary = addendum(AddendumWriter.summaryKey).toString
    (output, summary)
  }

}

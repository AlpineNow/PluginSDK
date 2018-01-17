package com.alpine.plugin.test.unittests

import java.io.File

import com.alpine.plugin.core.dialog.ChorusFile
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsDelimitedTabularDatasetDefault, IONoneDefault}
import com.alpine.plugin.core.visualization._
import com.alpine.plugin.test.mock._
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.Random


class FullOperatorTest extends SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

  private val testData = """2014,"",Volt,5000
                           |2015,,Volt,5000
                           |new,"",Volt,5000
                           |5.5,"",Volt,5000""".stripMargin
  private val testResultsFolder = "target/test-results"

  test("Check default values") {
    val uuid = "1"
    val inputDataName = "TestData"

    val inputRows = sc.parallelize(List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23)))
    val inputSchema =
      StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val dataFrameInput = sparkSession.createDataFrame(inputRows, inputSchema)

    val parameters = new OperatorParametersMock(inputDataName, uuid)
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "name")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "name", "age")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "ColumnSelector")

    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, _) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, dataFrameInput)
    assert(r.collect().nonEmpty)
  }

  test("DF Template Saving ") {
    val testDataFilePath = new File(testResultsFolder, s"test-data${Random.nextInt()}.csv")
    FileUtils.write(testDataFilePath, testData)
    testDataFilePath.deleteOnExit()
    val carsSchema = new StructType(
      Array(
        StructField("year", IntegerType, nullable = true),
        StructField("make", StringType, nullable = true),
        StructField("model", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true)
      ))
    val input = HdfsDelimitedTabularDatasetDefault(testDataFilePath.getPath,
      sparkUtils.convertSparkSQLSchemaToTabularSchema(carsSchema),
      TSVAttributes.defaultCSV)
    val parameters = new OperatorParametersMock("1", "CarData")
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "year")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "make")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "CarsTestOutput")
    val o = runInputThroughEntireOperator(input, new TestGui, new TestSparkJob, parameters)
    val outputAsDelimited = o.asInstanceOf[HdfsDelimitedTabularDataset]
    assert(outputAsDelimited.tsvAttributes == TSVAttributes.defaultCSV)
  }

  test("Check adding only Some values to Multiple column Selector ") {
    val golfData = GolfData.createGolfDF(sparkSession)

    val parameters = new OperatorParametersMock("2", "GolfData")
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "play")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "outlook")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "GolfTestOutput")
    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, addendum) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, golfData)
    assert(r.collect().nonEmpty)
  }

  test("Test Non DataFrameOperator and Addendum Utils") {
    //for downloading a workflow
    //pretending that the text of the workflow is stored at this path
    val workFilePath = new File(testResultsFolder, s"PigDates${Random.nextInt()}.csv")
    val testData = """2016-09-06T09:46:44.191-07:00,2016-09-06T09:46:44.222-07:00
                     |2016-09-06T09:46:44.191-07:00,2016-09-06T09:46:44.222-07:00""".stripMargin
    FileUtils.write(workFilePath, testData)
    workFilePath.deleteOnExit()
    val workfile = ChorusFile("PigDates", "1")
    //These are the workfile that the chorus api caller will have access to.
    //They will also be used for validation in the mock operator dialog class
    val workfilesInWorkspace = Seq(ChorusFileInWorkspaceMock(workfile, Some(workFilePath.getPath)))
    val inputChorusAPICaller = new ChorusAPICallerMock(workfilesInWorkspace)

    val inputParams = new OperatorParametersMock("test operator", "uuid")
    inputParams.setChorusFile("addChorusFileDropdownBox", workfile)
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParams, "result")
    val gui = new HelloWorldInSparkGUINode
    val input = IONoneDefault()
    val output: HdfsDelimitedTabularDataset =
      runInputThroughEntireOperator(input, gui, new HelloWorldInSparkJob, inputParams)
    assert(output.tsvAttributes == TSVAttributes.defaultCSV, "The dataframe template should return CSV results by default")

    val visualModel = new HelloWorldRuntime().createVisualResults(
      new SparkExecutionContextMock(inputChorusAPICaller), input, output, inputParams, new SimpleOperatorListener()
    )
    val models = visualModel.asInstanceOf[CompositeVisualModel].subModels

    assert(models.toMap.apply("Workfile").asInstanceOf[TextVisualModel].content.contains("2016-09-06T09"))
    assert(models.unzip._1 == Seq("Output", "ModelOne", "ModelTwo", "ModelThree", "Workfile", "Summary"))
    assert(models.toMap.apply("Summary").asInstanceOf[HtmlVisualModel].html == "Some Sample Summary Text")
  }
}

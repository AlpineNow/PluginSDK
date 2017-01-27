package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ChorusFile, ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsDelimitedTabularDatasetDefault, IONoneDefault}
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.{AddendumWriter, HdfsParameterUtils}
import com.alpine.plugin.core.visualization._
import com.alpine.plugin.core.{OperatorGUINode, OperatorListener, OperatorParameters}
import com.alpine.plugin.test.mock.ChorusAPICallerMock.ChorusFileInWorkspaceMock
import com.alpine.plugin.test.mock.{ChorusAPICallerMock, OperatorParametersMock, VisualModelFactoryMock}
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable


class TestGui extends SparkDataFrameGUINode[TestSparkJob] {

  override def onPlacement(operatorDialog: OperatorDialog,
                           operatorDataSourceManager: OperatorDataSourceManager,
                           operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addCheckboxes(
      id = "addCheckboxes",
      label = "addCheckboxes Param",
      values = Seq(
        "red fish",
        "blue fish",
        "one fish",
        "two fish"
      ),
      defaultSelections = Seq("blue fish")
    )

    operatorDialog.addTabularDatasetColumnCheckboxes(
      id = "addTabularDatasetColumnCheckboxes",
      label = "addTabularDatasetColumnCheckboxes Param",
      columnFilter = ColumnFilter.All,
      selectionGroupId = "main"
    )

    operatorDialog.addTabularDatasetColumnDropdownBox(
      id = "addTabularDatasetColumnDropdownBox",
      label = "addTabularDatasetColumnDropdownBox Param",
      columnFilter = ColumnFilter.All,
      selectionGroupId = "other"
    )

    operatorDialog.addIntegerBox("addIntegerBox", " addIntegerBox Param", 0, 100, 50)

    operatorDialog.addStringBox("addStringBox", "addStringBox Param", "aaa", "a*",
      required = true)

    operatorDialog.addDoubleBox("addDoubleBox", "addDoubleBox Param", 0.0, 1.0,
      inclusiveMin = true, inclusiveMax = true, 0.5)

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

}

class TestSparkJob extends SparkDataFrameJob {

  override def transformWithAddendum(operatorParameters: OperatorParameters, dataFrame: DataFrame,
                                     sparkUtils: SparkRuntimeUtils, listener: OperatorListener) = {
    val col: String = operatorParameters.getTabularDatasetSelectedColumn("addTabularDatasetColumnDropdownBox")._2
    val col2: Array[String] = operatorParameters.getTabularDatasetSelectedColumns("addTabularDatasetColumnCheckboxes")._2

    val checkboxes: Array[String] = operatorParameters.getStringArrayValue("addCheckboxes")
    assert(checkboxes.sameElements(Seq("blue fish")))
    val stringParam = operatorParameters.getStringValue("addStringBox")
    assert(stringParam == "aaa")
    val intParam = operatorParameters.getIntValue("addIntegerBox")
    assert(intParam == 50)
    val doubleParam = operatorParameters.getDoubleValue("addDoubleBox")

    assert(doubleParam == 0.5)
    (dataFrame.select(col, col2: _ *),
      AddendumWriter.createStandardAddendum("Summary of the operator"))
  }
}

class FullOperatorTests extends SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

  test("Check default values") {
    val uuid = "1"
    val inputDataName = "TestData"

    val inputRows = sc.parallelize(List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23)))
    val inputSchema =
      StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val dataFrameInput = sqlContext.createDataFrame(inputRows, inputSchema)

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
    val path = "plugin-test/src/test/resources/TestData.csv"
    val carsSchema = new StructType(
      Array(
        StructField("year", IntegerType, nullable = true),
        StructField("make", StringType, nullable = true),
        StructField("model", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true)
      ))
    val input = HdfsDelimitedTabularDatasetDefault(path,
      sparkUtils.convertSparkSQLSchemaToTabularSchema(carsSchema),
      TSVAttributes.defaultCSV)
    val parameters = new OperatorParametersMock("1", "CarData")
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "year")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "make")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "CarsTestOutput")
    val o = runInputThroughEntireOperator(input, new TestGui, new TestSparkJob, parameters, Some(input.tabularSchema))
    val outputAsDelimited = o.asInstanceOf[HdfsDelimitedTabularDataset]
    assert(outputAsDelimited.tsvAttributes == TSVAttributes.defaultCSV)
  }

  test("Check adding only Some values to Multiple column Selector ") {
    val golfData = GolfData.createGolfDF(sc)

    val parameters = new OperatorParametersMock("2", "GolfData")
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "play")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "outlook")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "GolfTestOutput")
    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, addendum) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, golfData)
    assert(r.collect().nonEmpty)
  }

  class HelloWorldInSparkGUINode extends OperatorGUINode[
    IONone,
    HdfsDelimitedTabularDataset] {
    override def onPlacement(
                              operatorDialog: OperatorDialog,
                              operatorDataSourceManager: OperatorDataSourceManager,
                              operatorSchemaManager: OperatorSchemaManager): Unit = {

      operatorDialog.addChorusFileDropdownBox(
        id = "addChorusFileDropdownBox",
        label = " " +
          "addChorusFileDropdownBox Param",
        extensionFilter = Set(".txt", ".afm"),
        isRequired = true)

      HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

      val outputSchema =
        TabularSchema(Array(ColumnDef("HelloWorld", ColumnType.String)))
      operatorSchemaManager.setOutputSchema(outputSchema)
    }

    override def onOutputVisualization(params: OperatorParameters, output: HdfsDelimitedTabularDataset,
                                       visualFactory: VisualModelFactory): VisualModel = {

      val chorusWorkFile = params.getChorusFile("addChorusFileDropdownBox")
      val chorusAPICaller = params.getChorusAPICaller
      val downloadWorkFile = chorusAPICaller.getWorkfileAsInputStream(chorusWorkFile.fileId).get
      val workfileText = scala.io.Source.fromInputStream(downloadWorkFile).mkString

      val additionalModels: Array[(String, VisualModel)] = Array(
        ("ModelOne", TextVisualModel("One Fish")),
        ("ModelTwo", TextVisualModel("Two Fish")),
        ("ModelThree", TextVisualModel("Three Fish")),
        ("Workfile", TextVisualModel(workfileText))
      )

      AddendumWriter.createCompositeVisualModel(visualFactory, output, additionalModels)
    }

  }

  class HelloWorldInSparkJob extends SparkIOTypedPluginJob[
    IONone,
    HdfsDelimitedTabularDataset] {
    override def onExecution(
                              sparkContext: SparkContext,
                              appConf: mutable.Map[String, String],
                              input: IONone,
                              operatorParameters: OperatorParameters,
                              listener: OperatorListener): HdfsDelimitedTabularDataset = {

      val outputPathStr = HdfsParameterUtils.getOutputPath(operatorParameters)

      val outputSchema = TabularSchema(Array(ColumnDef("HelloWorld", ColumnType.String)))

      HdfsDelimitedTabularDatasetDefault(
        outputPathStr,
        outputSchema,
        TSVAttributes.defaultCSV,
        AddendumWriter.createStandardAddendum("Some Sample Summary Text")
      )
    }
  }

  test("Test Non DataFrameOperator and Addendum Utils") {
    //for downloading a workflow
    //pretending that the text of the workflow is stored at this path
    val workFilePath = "plugin-test/src/test/resources/PigDates.csv"
    val workfile = ChorusFile("PigDates", "1")
    //These are the workfile that the chorus api caller will have access to.
    //They will also be used for validation in the mock operator dialog class
    val workfilesInWorkspace =
    Seq(ChorusFileInWorkspaceMock(workfile, Some(workFilePath), None))
    val inputChorusAPICaller =
      new ChorusAPICallerMock(workfilesInWorkspace)

    val inputParams = new OperatorParametersMock("test operator", "uuid", inputChorusAPICaller)
    inputParams.setValue("addChorusFileDropdownBox", workfile)
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParams, "result")
    val gui = new HelloWorldInSparkGUINode
    val output: HdfsDelimitedTabularDataset = runInputThroughEntireOperator(IONoneDefault(),
      gui, new HelloWorldInSparkJob, inputParams, None)
    assert(output.tsvAttributes == TSVAttributes.defaultCSV,
      "The dataframe template should return CSV results by default")
    val visualModel = gui.onOutputVisualization(inputParams, output, new VisualModelFactoryMock)
    val models = visualModel.asInstanceOf[CompositeVisualModel].subModels

    assert(models.toMap.apply("Workfile").asInstanceOf[TextVisualModel].content.contains("2016-09-06T09"))
    assert(models.unzip._1 == Seq("Output", "ModelOne", "ModelTwo", "ModelThree", "Workfile", "Summary"))
    assert(models.toMap.apply("Summary").asInstanceOf[HtmlVisualModel].html == "Some Sample Summary Text")
  }
}

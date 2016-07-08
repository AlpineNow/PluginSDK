package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsDelimitedTabularDatasetDefault, IONoneDefault}
import com.alpine.plugin.core.spark.SparkIOTypedPluginJob
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.{AddendumWriter, HdfsParameterUtils}
import com.alpine.plugin.core.visualization._
import com.alpine.plugin.core.{OperatorGUINode, OperatorListener, OperatorParameters}
import com.alpine.plugin.test.mock.{OperatorParametersMock, VisualModelFactoryMock}
import com.alpine.plugin.test.utils.{GolfData, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable


class TestGui extends SparkDataFrameGUINode[TestSparkJob] {

  override def onPlacement(operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    operatorDialog.addCheckboxes(
      id = "addCheckboxes",
      label = "addCheckboxes param",
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
      label = "addTabularDatasetColumnCheckboxes Param ",
      columnFilter = ColumnFilter.All,
      selectionGroupId = "main"
    )

    operatorDialog.addTabularDatasetColumnDropdownBox(
      id = "addTabularDatasetColumnDropdownBox",
      label = "addTabularDatasetColumnDropdownBox param",
      columnFilter = ColumnFilter.All,
      selectionGroupId = "other"
    )

    operatorDialog.addIntegerBox("addIntegerBox", " addIntegerBox param ", 0, 100, 50)

    operatorDialog.addStringBox("addStringBox", "addStringBox Param", "aaa", "a*", 0, 0)

    operatorDialog.addDoubleBox("addDoubleBox", "addDoubleBox param", 0.0, 1.0,
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
    (dataFrame.select(col, col2 : _ *), AddendumWriter.createStandardAddendum("Summary of the operator"))
  }
}

class FullOperatorTests extends  SimpleAbstractSparkJobSuite {

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

  test("Check adding only Some values to Multiple column Selector "){
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

      HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)

      val outputSchema =
        TabularSchema(Array(ColumnDef("HelloWorld", ColumnType.String)))
      operatorSchemaManager.setOutputSchema(outputSchema)
    }

    override def onOutputVisualization(params: OperatorParameters, output: HdfsDelimitedTabularDataset,
      visualFactory: VisualModelFactory): VisualModel = {
      val additionalModels: Array[(String, VisualModel)] = Array(
        ("ModelOne", visualFactory.createTextVisualization("One Fish")),
        ("ModelTwo", visualFactory.createTextVisualization("Two Fish")),
        ("ModelThree", visualFactory.createHtmlTextVisualization("Three Fish")))
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

      new HdfsDelimitedTabularDatasetDefault(
        outputPathStr,
        outputSchema,
        TSVAttributes.default,
        Some(operatorParameters.operatorInfo),
        AddendumWriter.createStandardAddendum("Some Sample Summary Text")
      )
    }
  }

  test("Test Non DataFrameOperator and Addendum Utils"){
    val inputParams = new OperatorParametersMock("3", "r")
    OperatorParameterMockUtil.addHdfsParamsDefault(inputParams, "result")
    val gui = new HelloWorldInSparkGUINode
    val output: HdfsDelimitedTabularDataset = runInputThroughEntireOperator(new IONoneDefault(),
      gui, new HelloWorldInSparkJob,
    inputParams, None )

    val visualModel = gui.onOutputVisualization(inputParams, output, new VisualModelFactoryMock)
    val models = visualModel.asInstanceOf[CompositeVisualModel].subModels

    assert(models.unzip._1 == Seq("Output", "ModelOne", "ModelTwo", "ModelThree", "Summary"))
    assert(models.toMap.apply("Summary").asInstanceOf[HtmlVisualModel].html == "Some Sample Summary Text")
    assert(output.tsvAttributes == TSVAttributes.default)
  }
}

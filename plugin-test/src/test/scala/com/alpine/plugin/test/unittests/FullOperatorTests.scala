package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{IONoneDefault, HdfsDelimitedTabularDatasetDefault}
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.{OperatorGUINode, OperatorListener, OperatorParameters}
import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{GolfData, TestSparkContexts, OperatorParameterMockUtil, SimpleAbstractSparkJobSuite}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable


class FullOperatorTests extends  SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

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

    override def transform(operatorParameters: OperatorParameters, dataFrame: DataFrame,
                           sparkUtils: SparkRuntimeUtils, listener: OperatorListener): DataFrame = {
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
      dataFrame.select(col, col2 : _ *)
    }
  }

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
    OperatorParameterMockUtil.addHdfsParams(parameters, "ColumnSelector")

    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, _) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, dataFrameInput)
    assert(r.collect().nonEmpty)
  }

  test("Check adding only Some values to Multiple column Selector "){
    val golfData = GolfData.createGolfDF(sc)
    val parameters = new OperatorParametersMock("2", "GoldData")
    OperatorParameterMockUtil.addTabularColumn(parameters, "addTabularDatasetColumnDropdownBox", "play")
    OperatorParameterMockUtil.addTabularColumns(parameters, "addTabularDatasetColumnCheckboxes", "outlook")
    OperatorParameterMockUtil.addHdfsParams(parameters, "GolfTestOutput")
    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, _) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, golfData)
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
        Some(operatorParameters.operatorInfo)
      )
    }
  }

  test("Test Non DataFrameOperator"){
    val inputParams = new OperatorParametersMock("3", "r")
    OperatorParameterMockUtil.addHdfsParams(inputParams, "result")

    val output: HdfsDelimitedTabularDataset = runInputThroughEntireOperator(new IONoneDefault(), new HelloWorldInSparkGUINode, new HelloWorldInSparkJob,
    inputParams, None )

    assert(output.tsvAttributes == TSVAttributes.default)

  }


}

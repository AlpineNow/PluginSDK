package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.dialog.{IRowDialogRow, IRowDialogValue}
import com.alpine.plugin.core.spark.templates.SparkDataFrameJob
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.{HdfsParameterUtils, HdfsStorageFormatType}
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.test.mock.{IRowDialogRowMock, IRowDialogRowValueMock, OperatorParametersMock, SimpleOperatorListener}
import com.alpine.plugin.test.utils.{OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class TestOfTestUtils extends SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

  test("Test Methods In SimpleAbstractSpark Job Suite with Column Selector Plugin") {
    val columnParamId = "col"
    class testOperatorJob extends SparkDataFrameJob {

      override def transform(operatorParameters: OperatorParameters, dataFrame: DataFrame,
                             sparkUtils: SparkRuntimeUtils, listener: OperatorListener): DataFrame = {
        val (_, colParam) = operatorParameters.getTabularDatasetSelectedColumn(columnParamId)
        dataFrame.select(colParam)
      }
    }

    val inputRows = List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23))
    val inputSchema =
      StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val operator = new testOperatorJob
    val input = sc.parallelize(inputRows)

    //create a dataFrame using that test data
    val dataFrameInput = sparkSession.createDataFrame(input, inputSchema)
    val uuid = "1"
    val colFilterName = "TestColumnSelector"
    val parameters = new OperatorParametersMock(colFilterName, uuid)

    val expectedRows = Array(Row("Masha"), Row("Ulia"), Row("Nastya"))

    OperatorParameterMockUtil.addTabularColumn(parameters, columnParamId, "name")
    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "ColumnSelector")

    val result = operator.transform(parameters, dataFrame = dataFrameInput,
      sparkUtils, new SimpleOperatorListener)
    assert(!result.schema.fieldNames.contains("age"))

    assert(result.collect().sameElements(expectedRows))
  }

  test("Test add Parameters methods") {
    val outputDir = "dir"
    val outputName = "name"
    val operatorParametersMock = new OperatorParametersMock("123", "thing")
    OperatorParameterMockUtil.addHdfsParams(
      operatorParametersMock = operatorParametersMock,
      outputName = outputName,
      outputDirectory = outputDir,
      storageFormat = HdfsStorageFormatType.CSV,
      overwrite = false)

    //test that we can use HdfsParameterUtils to retrieve all these parameters
    val path = HdfsParameterUtils.getOutputPath(operatorParametersMock)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParametersMock)
    assert(path.equals(outputDir + '/' + outputName))
    assert(!overwrite)
  }

  test("Test Add single Column Method") {
    val operatorParametersMock = new OperatorParametersMock("123", "thing")
    OperatorParameterMockUtil.addTabularColumn(operatorParametersMock, "id", "colName")
    val (_, parameterValue) = operatorParametersMock.getTabularDatasetSelectedColumn("id")
    assert(parameterValue.equals("colName"))
  }

  test("Test Add multiple Column Method") {
    val operatorParametersMock = new OperatorParametersMock("123", "thing")
    OperatorParameterMockUtil.addTabularColumns(operatorParametersMock, "id", "col1", "col2")
    val (_, parameterValue) = operatorParametersMock.getTabularDatasetSelectedColumns("id")
    assert(parameterValue.toSet.equals(Set("col1", "col2")))
  }

  test("Test Add RowDialogPopup Method") {
    val operatorParametersMock = new OperatorParametersMock("123", "smth")
    val firstRowValues = List(IRowDialogRowValueMock("elem_1", "val_11" ), IRowDialogRowValueMock("elem_2", "val_12"))
    val secondRowValues = List(IRowDialogRowValueMock("elem_1", "val_21" ), IRowDialogRowValueMock("elem_2", "val_22"))
    OperatorParameterMockUtil.addRowDialogElements(operatorParametersMock, "id", IRowDialogRowMock(firstRowValues), IRowDialogRowMock(secondRowValues))

    val parameterRowValues: Array[IRowDialogRow] = operatorParametersMock.getDialogRowsAsArray("id")
    assert(parameterRowValues.map(_.getRowDialogValues).sameElements(Array(firstRowValues, secondRowValues)))
  }

}
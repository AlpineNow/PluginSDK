package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io.OperatorSchemaManager
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.{BadDataReportingUtils, SparkRuntimeUtils}
import com.alpine.plugin.core.utils.HdfsParameterUtils
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.test.mock.OperatorParametersMock
import com.alpine.plugin.test.utils.{OperatorParameterMockUtil, SimpleAbstractSparkJobSuite, TestSparkContexts}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class TestNewStorageFormatMethods extends SimpleAbstractSparkJobSuite {

  import TestSparkContexts._

  class TestGui extends SparkDataFrameGUINode[TestSparkJob] {

    override def onPlacement(operatorDialog: OperatorDialog,
                             operatorDataSourceManager: OperatorDataSourceManager,
                             operatorSchemaManager: OperatorSchemaManager): Unit = {


      super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
    }
  }

  class TestSparkJob extends SparkDataFrameJob {

    override def transform(operatorParameters: OperatorParameters, dataFrame: DataFrame,
                           sparkUtils: SparkRuntimeUtils, listener: OperatorListener): DataFrame = {
      val storageFormat = HdfsParameterUtils.getHdfsStorageFormatType(operatorParameters)
      val writeBadDataToFile = BadDataReportingUtils.handleNullDataAsDataFrameDefault(None,
        "target/testResults/", 10, 10, None, sparkUtils)
      dataFrame
    }
  }

  test("Check default values") {
    val uuid = "1"
    val inputDataName = "TestData"

    val inputRows = sc.parallelize(List(Row("Masha", 22), Row("Ulia", 21), Row("Nastya", 23)))
    val inputSchema =
      StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val dataFrameInput = sparkSession.createDataFrame(inputRows, inputSchema)

    val parameters = new OperatorParametersMock(inputDataName, uuid)

    OperatorParameterMockUtil.addHdfsParamsDefault(parameters, "ColumnSelector")

    val operatorGUI = new TestGui
    val operatorJob = new TestSparkJob
    val (r, _) = runDataFrameThroughEntireDFTemplate(operatorGUI, operatorJob, parameters, dataFrameInput)
    assert(r.collect().nonEmpty)
  }

}


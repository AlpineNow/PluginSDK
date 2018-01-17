package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.{OperatorListener, OperatorParameters}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ColumnFilter, OperatorDialog}
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.reporting.NullDataReportingUtils
import com.alpine.plugin.core.spark.{AlpineSparkEnvironment, SparkIOTypedPluginJob}
import com.alpine.plugin.core.spark.templates.{SparkDataFrameGUINode, SparkDataFrameJob}
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils.{AddendumWriter, HdfsParameterUtils}
import org.apache.spark.sql.DataFrame

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

    HdfsParameterUtils.addNullDataReportParameter(operatorDialog)

    super.onPlacement(operatorDialog, operatorDataSourceManager, operatorSchemaManager)
  }

}

class TestSparkJob extends SparkDataFrameJob {

  override def transformWithAddendum(operatorParameters: OperatorParameters, dataFrame: DataFrame,
                                     sparkUtils: SparkRuntimeUtils, listener: OperatorListener): (DataFrame, Map[String, AnyRef]) = {
    val col: String = operatorParameters.getTabularDatasetSelectedColumn("addTabularDatasetColumnDropdownBox")._2
    val col2: Array[String] = operatorParameters.getTabularDatasetSelectedColumns("addTabularDatasetColumnCheckboxes")._2

    val nullDataReportingStrategy = HdfsParameterUtils.getNullDataStrategy(operatorParameters, None)
    val condition = dataFrame(col).isNull
    val (goodDF, msg) = NullDataReportingUtils.filterNullDataAndReportGeneral(dataFrame, sparkUtils, nullDataReportingStrategy, condition,
      "because prediction column is null")



    val checkboxes: Array[String] = operatorParameters.getStringArrayValue("addCheckboxes")
    assert(checkboxes.sameElements(Seq("blue fish")))
    val stringParam = operatorParameters.getStringValue("addStringBox")
    assert(stringParam == "aaa")
    val intParam = operatorParameters.getIntValue("addIntegerBox")
    assert(intParam == 50)
    val doubleParam = operatorParameters.getDoubleValue("addDoubleBox")
    assert(doubleParam == 0.5)
    (goodDF.select(col, col2: _ *),
      AddendumWriter.createStandardAddendum(msg))
  }
}


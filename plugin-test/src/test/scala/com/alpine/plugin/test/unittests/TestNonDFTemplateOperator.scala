package com.alpine.plugin.test.unittests

import com.alpine.plugin.core.{OperatorGUINode, OperatorListener, OperatorParameters}
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.{ChorusFile, OperatorDialog}
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.spark.{AlpineSparkEnvironment, SparkExecutionContext, SparkIOTypedPluginJob, SparkRuntime}
import com.alpine.plugin.core.utils.{AddendumWriter, HdfsParameterUtils}
import com.alpine.plugin.core.visualization.{TextVisualModel, VisualModel}

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

  }

  class HelloWorldRuntime extends SparkRuntime[IONone, HdfsDelimitedTabularDataset] {

    override def onExecution(context: SparkExecutionContext, input: IONone,
      params: OperatorParameters, listener: OperatorListener): HdfsDelimitedTabularDataset = ???

    override def onStop(context: SparkExecutionContext, listener: OperatorListener): Unit = ???

    override def createVisualResults(context: SparkExecutionContext,
                                     input: IONone,
                                     output: HdfsDelimitedTabularDataset,
                                     params: OperatorParameters,
                                     listener: OperatorListener): VisualModel = {
      val chorusWorkFile: Option[ChorusFile] = params.getChorusFile("addChorusFileDropdownBox")
      val workfileText: String = context.chorusAPICaller.readWorkfileAsText(chorusWorkFile.get).get

      val additionalModels: Array[(String, VisualModel)] = Array(
        ("ModelOne", TextVisualModel("One Fish")),
        ("ModelTwo", TextVisualModel("Two Fish")),
        ("ModelThree", TextVisualModel("Three Fish")),
        ("Workfile", TextVisualModel(workfileText))
      )

      AddendumWriter.createCompositeVisualModel(context.visualModelHelper, output, additionalModels)

    }
  }

  class HelloWorldInSparkJob extends SparkIOTypedPluginJob[
    IONone,
    HdfsDelimitedTabularDataset] {
    override def onExecution(
                              alpineSparkEnvironment: AlpineSparkEnvironment,
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
/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.spark.templates

import scala.collection.mutable

import com.alpine.plugin.core._
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.utils._
import com.alpine.plugin.core.spark.{SparkIOTypedPluginJob, SparkRuntimeWithIOTypedJob}
import com.alpine.plugin.core.utils.{HdfsStorageFormatType, HdfsParameterUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
 * Templated base for Spark plugin jobs operating on DataFrames.
 * Most jobs will want to use SparkDataFrameJob which takes
 * and returns Spark DataFrames. This version does not support schema
 * inference.
 * @tparam ReturnType The return type of the transformation method (most commonly a DataFrame)
 * @tparam OutputType The return type of the actual operator, extending IOBase.
 *                    Most commonly will be an HDFS dataset of some flavor (see SparkDataFrame)
 */

abstract class TemplatedSparkDataFrameJob[ReturnType, OutputType <: IOBase]
  extends SparkIOTypedPluginJob[HdfsTabularDataset, OutputType] {

  override def onExecution(sparkContext: SparkContext,
    appConf: mutable.Map[String, String],
    input: HdfsTabularDataset,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): OutputType = {
    val sparkUtils = new SparkRuntimeUtils(sparkContext)
    val dataFrame = sparkUtils.getDataFrame(input)
    listener.notifyMessage("Starting transformation")
    val (results, addendum) = transformWithAddendum(operatorParameters,
      dataFrame, sparkUtils, listener)
    val storageFormat = operatorParameters.getStringValue("storageFormat")
    val outputPath = HdfsParameterUtils.getOutputPath(operatorParameters)
    listener.notifyMessage(s"Saving results to $outputPath in $storageFormat")
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)
    val output = saveResults(
      results, sparkUtils, storageFormat, outputPath, overwrite,
      Some(operatorParameters.operatorInfo), addendum, TSVAttributes.default
    )
    output
  }

  /**
   * Define the transformation from the input dataset, expressed as a dataFrame, where the
   * schema corresponds to the Alpine column header to the output dataset, of type 'ReturnType'.
   * In addition return a map of type String -> AnyRef (Object in java) which will be added to
   * the output and used in the GUI node to return additional output or define visualization.
   * Default implementation returns the input DataFrame with no Addendum information.
   * If you use this version schema inference will not work.
   * @param dataFrame - the input data
   * @param sparkUtils - a sparkUtils object including the Spark context
   * @param listener - the operator listener object which can be used to print messages to the GUI.
   * @return the output DataFrame and a map containing the keys and values to add to the output
   */
  def transformWithAddendum(operatorParameters: OperatorParameters,
                                 dataFrame: DataFrame,
                                 sparkUtils: SparkRuntimeUtils,
                                 listener: OperatorListener) : (ReturnType, Map[String, AnyRef])

  /**
   * Write the results to the target path
   * @param results - the data to write
   * @param sparkUtils- Spark utils object with utility methods to write data and transform
   *                  between Alpine header types and Spark SQL schema types
   * @param storageFormat - Parquet, Avro, and TSV
   * @param path full HDFS output path
   * @param overwrite Boolean indicating whether to overwrite existing results at that location.
   * @return
   */
  def saveResults(results: ReturnType,
                  sparkUtils: SparkRuntimeUtils,
                  storageFormat: String,
                  path: String,
                  overwrite: Boolean,
                  sourceOperatorInfo: Option[OperatorInfo],
                  addendum: Map[String, AnyRef] = Map[String, AnyRef](),
                  tSVAttributes: TSVAttributes): OutputType

}

abstract class TemplatedSparkDataFrameRuntime[JobType <: TemplatedSparkDataFrameJob[_, O], O <: IOBase]
  extends SparkRuntimeWithIOTypedJob[JobType, HdfsTabularDataset, O] {
}

/**
 * Job base for non-inferred Spark plugin jobs taking and returning DataFrames.
 * Note: This WILL NOT work with hive.
 */
abstract class SparkDataFrameJob extends TemplatedSparkDataFrameJob[DataFrame, HdfsTabularDataset] {
  /**
   * Define the transformation from the input dataset, expressed as a dataFrame, where the
   * schema corresponds to the Alpine column header to the output dataset, also as a dataFrame.
   *
   * Override this method to define a DataFrame transformation, if you do not want to save
   * any  additional output (the default is to output the data frame and show a preiew of
   * the data frame as a visualization).
   * dataset). To define an addendum to create additional output use the 'TransformWithAddendum'
   * method.
   * If you use this version schema inference will not work.
   * @param dataFrame - the input data
   * @param sparkUtils - a sparkUtils object including the Spark context
   * @param listener - the operator listener object which can be used to print messages to the GUI.
   * @return your transformed DataFrame (Default implementation returns the input DataFrame)
   */
  def transform(operatorParameters: OperatorParameters,
                dataFrame: DataFrame,
                sparkUtils: SparkRuntimeUtils,
                listener: OperatorListener): DataFrame = {
     dataFrame
  }

  /**
   * Define the transformation from the input dataset, expressed as a dataFrame, where the
   * schema corresponds to the Alpine column header to the output dataset, as a dataData '.
   * In addition return a map of type String -> AnyRef (Object in java) which will be added to
   * the output.
   * @param dataFrame - the input data
   * @param sparkUtils - a sparkUtils object including the Spark context
   * @param listener - the operator listener object which can be used to print messages to the GUI.
   * @return the output DataFrame and a map containing the keys and values to add to the output.
   *         (Default implementation returns the input DataFrame with no Addendum information)
   */
   def transformWithAddendum(operatorParameters: OperatorParameters,
                                 dataFrame: DataFrame,
                                 sparkUtils: SparkRuntimeUtils,
                                 listener: OperatorListener) : (DataFrame, Map[String, AnyRef]) = {
    (transform(operatorParameters, dataFrame, sparkUtils, listener),  Map[String, AnyRef]())
  }

  /**
   * Writes the dataFrame to HDFS as either a Parquet dataset, Avro dataset, or tabular delimited
   * dataset.
   * @param transformedDataFrame The data frame that is to be stored to HDFS.
   * @param sparkUtils- contains utility methods to write data and to convert between Alpine header
   *                  types and
   *                  Spark SQL schemas
   * @param storageFormat - Parquet, Avro, and TSV
   * @param outputPath The location in HDFS to store the data frame.
   * @param overwrite - If false will throw a "File Already Exists" exception if the output path
   *                  already exists.
   *                  If true will delete the existing results before trying to write the new ones.
   * @return
   */
  override def saveResults(transformedDataFrame: DataFrame,
                           sparkUtils: SparkRuntimeUtils,
                           storageFormat: String,
                           outputPath: String,
                           overwrite: Boolean,
                           sourceOperatorInfo: Option[OperatorInfo],
                           addendum: Map[String, AnyRef] = Map[String, AnyRef](), tSVAttributes: TSVAttributes = TSVAttributes.default): HdfsTabularDataset = {
    sparkUtils.saveDataFrame(
      outputPath,
      transformedDataFrame,
      HdfsStorageFormatType.withName(storageFormat),
      overwrite,
      sourceOperatorInfo,
      addendum,
      tSVAttributes
    )
  }
}

/**
 * A class controlling the runtime behavior of your plugin.
 * To use the default implementation, which launches a Spark job according to the default
 * Spark settings  you will not need to add any code beyond the class definition with the
 * appropriate type parameters.
 * @tparam JobType your implementation of SparkDataFrameJob
 */
abstract class SparkDataFrameRuntime[JobType <: SparkDataFrameJob]
  extends SparkRuntimeWithIOTypedJob[JobType, HdfsTabularDataset, HdfsTabularDataset] {
}

/**
 * A class for plugins which will use Schema inference
 */
abstract class InferredSparkDataFrameJob extends SparkDataFrameJob {
  /**
   * Define the transformation from the input dataset, expressed as a dataframe, where the schema corresponds
   * to the alpine column header to the output dataset, also as a dataframe.
   * If you use this version schema inference will not work.
   *
   * @param dataFrame - the input data
   * @param sparkUtils - a sparkUtils object including the spark context
   * @param listener - the operator listener object which can be used to print messages to the GUI.
   * @return
   */
  override def transform(operatorParameters: OperatorParameters, dataFrame: DataFrame, sparkUtils: SparkRuntimeUtils,
    listener: OperatorListener): DataFrame = {
    transform(operatorParameters, dataFrame, listener)
  }

  /**
   * Define the transformation from the input dataset, expressed as a dataframe, where the schema corresponds
   * to the alpine column header to the output dataset, also as a dataframe.
   *
   * @param dataFrame - the input data
   * @param listener - the operator listener object which can be used to print messages to the GUI.
   * @return
   */
  def transform(operatorParameters: OperatorParameters, dataFrame: DataFrame,
    listener: OperatorListener): DataFrame
}

/**
 * Control the GUI of your Spark job, through this you can specify any visualization for the
 * output of your job, and what params the user will need to specify.
 */
abstract class TemplatedSparkDataFrameGUINode[StorageType <: IOBase]
  extends OperatorGUINode[HdfsTabularDataset, StorageType] {
}

/**
 * Control the GUI of your Spark job, through this you can specify
 * any visualization for the output of your job, and what params
 * the user will need to specify.
 */
abstract class SparkDataFrameGUINode[Job <: SparkDataFrameJob]()
  extends TemplatedSparkDataFrameGUINode[HdfsTabularDataset]() {

  /**
   * Defines the params the user will be able to select. The default
   * asks for desired output format & output location.
   */
  override def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager): Unit = {

    HdfsParameterUtils.addHdfsStorageFormatParameter(operatorDialog, HdfsStorageFormatType.TSV)
    HdfsParameterUtils.addStandardHdfsOutputParameters(operatorDialog)
  }

  /**
   * Override this method to define the output schema by assigning fixed column definitions.
   * If you want to have a variable number of output columns,
   * simply override the defineEntireOutputSchema method
   * The default implementation of this method returns the same columns as the input data.
   * @param inputSchema - the Alpine 'TabularSchema' for the input DataFrame
   * @param params The parameters of the operator, including values set by the user.
   * @return A list of Column definitions used to create the output schema
   */
  def defineOutputSchemaColumns(
    inputSchema: TabularSchema,
    params: OperatorParameters): Seq[ColumnDef] = {
    inputSchema.getDefinedColumns
  }

  /**
   * Override this method to define an output schema
   * in some way other than by defining an array of the fixed column definitions.
   */
  def defineEntireOutputSchema(
    inputSchema: TabularSchema,
    params: OperatorParameters) : TabularSchema= {
    val newCols = defineOutputSchemaColumns(inputSchema, params)
    val storageFormat = HdfsParameterUtils.getHdfsStorageFormatType(params)
    val newTabularFormatAttributes = HdfsParameterUtils.getTabularFormatAttributes(storageFormat)
    TabularSchema(newCols, newTabularFormatAttributes)
  }

  protected def updateOutputSchema(inputSchemas: Map[String, TabularSchema],
                                  params: OperatorParameters,
                                  operatorSchemaManager: OperatorSchemaManager
                                  ) : Unit = {
    assert(inputSchemas.size < 2,
      "SparkDataFrameGuiNode class requires only one input dataSet")
    if (inputSchemas.size == 1) {
      val outputSchema = defineEntireOutputSchema(inputSchemas.head._2, params)
      operatorSchemaManager.setOutputSchema(outputSchema)
    }
  }

  /**
   * Calls 'updateOutputSchema' when the parameters are changed
   * @group internals
   */
  override def onInputOrParameterChange(
    inputSchemas: Map[String, TabularSchema],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    this.updateOutputSchema(
      inputSchemas,
      params,
      operatorSchemaManager
    )

    OperatorStatus(isValid = true, msg = None)
  }
}

/**
 * Control the GUI of your Spark job, through this you can specify
 * any visualization for the output of your job, and what params
 * the user will need to specify. Uses the provided operator to generate
 * an updated schema, this should work for most operators but if not
 * (e.g. your operator doesn't handle empty data or output schema depends
 * on input data) then you will have to perform your own schema update.
 */
abstract class InferredSparkDataFrameGUINode[Job <: InferredSparkDataFrameJob]()(implicit m: scala.reflect.Manifest[Job]) extends
    SparkDataFrameGUINode[Job] {
  private lazy val localSparkContext = SparkContextSingleton.getOrCreate()
  lazy val localSqlContext = SQLContextSingleton.getOrCreate(localSparkContext)
  lazy val sparkRuntimeUtils = new SparkRuntimeUtils(localSparkContext)

  private class FakeListener() extends OperatorListener {
    override def notifyMessage(msg: String): Unit = {}
    override def notifyError(error: String): Unit = {}
    override def notifyProgress(id: String, prog: Float): Unit = {}
  }

  /**
   * Override this method to define an output schema
   * instead of using automatic inference.
   */
  override def defineEntireOutputSchema(inputSchema: TabularSchema,
    params: OperatorParameters) : TabularSchema = {
    val sparkSchema = sparkRuntimeUtils.convertTabularSchemaToSparkSQLSchema(inputSchema)
    val dataFrame = localSqlContext.createDataFrame(
      localSparkContext.emptyRDD[org.apache.spark.sql.Row], sparkSchema)
    // Apply the transformation and extract our resulting schema
    val operator = m.erasure.newInstance.asInstanceOf[Job]
    val outputSparkSchema = operator.transform(params, dataFrame, new FakeListener()).schema
    sparkRuntimeUtils.convertSparkSQLSchemaToTabularSchema(
      outputSparkSchema)
  }
}

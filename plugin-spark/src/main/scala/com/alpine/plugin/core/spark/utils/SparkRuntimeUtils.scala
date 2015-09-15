/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.spark.utils

import scala.collection.mutable

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsAvroDatasetDefault, HdfsDelimitedTabularDatasetDefault, HdfsParquetDatasetDefault}
import com.alpine.plugin.core.utils.HdfsStorageFormat
import com.databricks.spark.csv._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types._

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
class SparkRuntimeUtils(sc: SparkContext) {
  // ======================================================================
  // Schema conversion util functions.
  // ======================================================================

  /**
   * Converts an Alpine specific 'ColumnType' to the corresponding Saprk SQL specific type.
   * If no match can be found for the type, return a string type rather than throwing an exception.
   * used to define data frame schemas.
   * @param columnType
   * @return
   */
  def convertColumnTypeToSparkSQLDataType(columnType: ColumnType.TypeValue): DataType = {
    columnType match {
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.Float => FloatType
      case ColumnType.Double => DoubleType
      case ColumnType.String => StringType
      case ColumnType.DateTime => TimestampType
      case _ => StringType // Rather than throwing an exception, treat it as a string type.
      // throw new UnsupportedOperationException(columnType.toString + " is not supported.")
    }
  }

  /**
   * Converts from a Spark SQL data type to an Alpine-specific column type.
   * @param dataType
   * @return
   */
  def convertSparkSQLDataTypeToColumnType(dataType: DataType): ColumnType.TypeValue = {
    dataType match {
      case IntegerType => ColumnType.Int
      case LongType => ColumnType.Long
      case FloatType => ColumnType.Float
      case DoubleType => ColumnType.Double
      case StringType => ColumnType.String
      case DateType => ColumnType.DateTime
      case TimestampType => ColumnType.DateTime
      case _ =>
        throw new UnsupportedOperationException(
          "Spark SQL data type " + dataType.toString + " is not supported."
        )
    }
  }

  /**
   *Convert the Alpine 'TabularSchema' with column names and types to the equivalent Spark SQL
   * data frame header.
   * @param tabularSchema An Alpine 'TabularSchemaOutline' object with fixed column definitions
   *                      containing a name and Alpine specific type.
   * @return
   */
  def convertTabularSchemaToSparkSQLSchema(tabularSchema: TabularSchema): StructType = {
    StructType(
      tabularSchema.getDefinedColumns.map{
        columnDef =>
          StructField(
            columnDef.columnName,
            convertColumnTypeToSparkSQLDataType(columnDef.columnType),
            nullable = true
          )
      }
    )
  }

  /**
   * Converts from a Spark SQL schema to the Alpine 'TabularSchema' type. The 'TabularSchema'
   * object this method returns can be used to create any of the tabular Alpine IO types
   * (HDFSTabular dataset, dataTable etc.)
   * @param schema -a Spark SQL DataFrame schema
   * @return the equivalent Alpine schema for that dataset
   */
  def convertSparkSQLSchemaToTabularSchema(schema: StructType): TabularSchema = {
    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()

    val schemaItr = schema.iterator
    while (schemaItr.hasNext) {
      val colInfo = schemaItr.next()
      val colDef = ColumnDef(
        columnName = colInfo.name,
        columnType = convertSparkSQLDataTypeToColumnType(colInfo.dataType)
      )

      columnDefs += colDef
    }

    TabularSchema(columnDefs)
  }

  // ======================================================================
  // Storage util functions.
  // ======================================================================

  /**
   * Save a data frame to a path using the given storage format, and return
   * a corresponding HdfsTabularDataset object that points to the path.
   * @param path The path to which we'll save the data frame.
   * @param dataFrame The data frame that we want to save.
   * @param storageFormat The format that we want to store in.
   * @param overwrite Whether to overwrite any existing file at the path.
   * @param sourceOperatorInfo Mandatory source operator information to be included
   *                           in the output object.
   * @param addendum Mandatory addendum information to be included in the output
   *                 object.
   * @return After saving the data frame, returns an HdfsTabularDataset object.
   */
  def saveDataFrame(
    path: String,
    dataFrame: DataFrame,
    storageFormat: HdfsStorageFormat.HdfsStorageFormat,
    overwrite: Boolean,
    sourceOperatorInfo: Option[OperatorInfo],
    addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsTabularDataset = {

    if (overwrite) {
      deleteFilePathIfExists(path)
    }

    storageFormat match {
      case HdfsStorageFormat.Parquet =>
        saveAsParquet(
          path,
          dataFrame,
          sourceOperatorInfo,
          addendum
        )

      case HdfsStorageFormat.Avro =>
        saveAsAvro(
          path,
          dataFrame,
          sourceOperatorInfo,
          addendum
        )

      case HdfsStorageFormat.TSV =>
        saveAsTSV(
          path,
          dataFrame,
          sourceOperatorInfo,
          addendum
        )
    }
  }

  /**
   * Write a DataFrame to HDFS as a Parquet file, and return an instance of the
   * HDFSParquet IO base type which contains the Alpine 'TabularSchema' definition (created by
   * converting the DataFrame schema) and the path to the to the saved data.
   * @param path
   * @param dataFrame
   * @return
   */
  def saveAsParquet(path: String,
                    dataFrame: DataFrame,
                    sourceOperatorInfo: Option[OperatorInfo],
                    addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsParquetDataset = {
    dataFrame.saveAsParquetFile(path)
    val tabularSchema = convertSparkSQLSchemaToTabularSchema(dataFrame.schema)
    new HdfsParquetDatasetDefault(path, tabularSchema, sourceOperatorInfo, addendum)
  }

  /**
   * Write a DataFrame as an HDFSAvro dataset, and return the an instance of the Alpine
   * HDFSAvroDataset type which contains the  'TabularSchema' definition
   * (created by converting the DataFrame schema) and the path to the to the saved data.
   * @param path
   * @param dataFrame
   * @return
   */
  def saveAsAvro(path: String,
                 dataFrame: DataFrame,
                 sourceOperatorInfo: Option[OperatorInfo],
                 addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsAvroDataset = {
    dataFrame.save(path, "com.databricks.spark.avro")
    val tabularSchema = convertSparkSQLSchemaToTabularSchema(dataFrame.schema)
    new HdfsAvroDatasetDefault(path, tabularSchema, sourceOperatorInfo, addendum)
  }

  /**
   * Write a DataFrame to HDFS as a Tabular Delimited file, and return an instance of the Alpine
   * HDFSParquet type  which contains the Alpine 'TabularSchema' definition (created by converting
   * the DataFrame schema) and the path to the to the saved data.
   * @param path
   * @param dataFrame
   * @return
   */
  def saveAsTSV(path: String,
                dataFrame: DataFrame,
                sourceOperatorInfo: Option[OperatorInfo],
                addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsDelimitedTabularDataset = {
    dataFrame.saveAsCsvFile(
      path,
      Map(
        "delimiter" -> "\t",
        "escape" -> "\\",
        "quote" -> "\"",
        "header" -> "false"
      )
    )

    val tabularSchema = convertSparkSQLSchemaToTabularSchema(dataFrame.schema)
    new HdfsDelimitedTabularDatasetDefault(
      path,
      tabularSchema,
      TSVAttributes.default,
      sourceOperatorInfo,
      addendum
    )
  }

  /**
   * Checks if the given file path already exists (and would cause a 'PathAlreadyExists'
   * exception when we try to write to it) and deletes the directory to prevent existing
   * results at that path if they do exist.
   * @param outputPathStr - the full HDFS path
   * @return
   */
  def deleteFilePathIfExists(outputPathStr : String) = {
    val outputPath = new Path(outputPathStr)
    val driverHdfs = FileSystem.get(sc.hadoopConfiguration)
    if (driverHdfs.exists(outputPath)) {
      println("The path exists already.")
      driverHdfs.delete(outputPath, true)
    }
  }

  // ======================================================================
  // Dataframe util functions.
  // ======================================================================

  /**
   * Returns a DataFrame from an Alpine HdfsTabularDataset. The DataFrame's schema will
   * correspond to the column header of the Alpine dataset.
   * @param dataset
   * @return Spark SQL DataFrame
   */
  def getDataFrame(dataset: HdfsTabularDataset): DataFrame = {
    // TODO : Sort out reusing SQL context for fast run. Right now this method
    // TODO : is causing failures, just creating SQL context from Spark context
    // TODO : right now.
    // val sqlContext = SQLContextSingleton.getOrCreate(sc)
    val sqlContext = new SQLContext(sc)
    val path = dataset.path

    if (dataset.isInstanceOf[HdfsAvroDataset]) {
      sqlContext.load(path, "com.databricks.spark.avro")
    } else if (dataset.isInstanceOf[HdfsParquetDataset]) {
      sqlContext.load(path)
    } else {
      val tabularSchema = dataset.tabularSchema
      val delimitedDataset = dataset.asInstanceOf[HdfsDelimitedTabularDataset]
      val tsvAttributes: TSVAttributes = delimitedDataset.tsvAttributes
      val schema: StructType = convertTabularSchemaToSparkSQLSchema(tabularSchema)

      new CsvParser().
        withUseHeader(tsvAttributes.containsHeader).
        withDelimiter(tsvAttributes.delimiter).
        withQuoteChar(tsvAttributes.quoteStr).
        withEscape(tsvAttributes.escapeStr).
        withSchema(schema).
        csvFile(sqlContext, path)
    }
  }

  /**
   * For use with hive. Returns a Spark data frame given a hive table.
   */
  def getDataFrame(dataset: HiveTable): DataFrame = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.table(dataset.getConcatenatedName)
  }
}

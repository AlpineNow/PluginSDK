/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.spark.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.io.defaults.{HdfsAvroDatasetDefault, HdfsDelimitedTabularDatasetDefault, HdfsParquetDatasetDefault}
import com.alpine.plugin.core.spark.OperatorFailedException
import com.alpine.plugin.core.utils.{HdfsStorageFormat, HdfsStorageFormatType}
import com.databricks.spark.csv._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}

import scala.util.Try

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
class SparkRuntimeUtils(sc: SparkContext) extends SparkSchemaUtils {

  val driverHdfs = FileSystem.get(sc.hadoopConfiguration)

  // ======================================================================
  // Storage util functions.
  // ======================================================================

  /**
    * Save a data frame to a path using the given storage format, and return
    * a corresponding HdfsTabularDataset object that points to the path.
    *
    * @param path               The path to which we'll save the data frame.
    * @param dataFrame          The data frame that we want to save.
    * @param storageFormat      The format that we want to store in.
    * @param overwrite          Whether to overwrite any existing file at the path.
    * @param sourceOperatorInfo Mandatory source operator information to be included
    *                           in the output object.
    * @param addendum           Mandatory addendum information to be included in the output
    *                           object.
    * @return After saving the data frame, returns an HdfsTabularDataset object.
    */
  def saveDataFrame[T <: HdfsStorageFormatType](
                                                 path: String,
                                                 dataFrame: DataFrame,
                                                 storageFormat: T,
                                                 overwrite: Boolean,
                                                 sourceOperatorInfo: Option[OperatorInfo],
                                                 addendum: Map[String, AnyRef],
                                                 tSVAttributes: TSVAttributes): HdfsTabularDataset = {

    deleteOrFailIfExists(path, overwrite)

    storageFormat match {
      case HdfsStorageFormatType.Parquet =>
        saveAsParquet(
          path,
          dataFrame,
          sourceOperatorInfo,
          addendum
        )

      case HdfsStorageFormatType.Avro =>
        saveAsAvro(
          path,
          dataFrame,
          sourceOperatorInfo,
          addendum
        )

      case HdfsStorageFormatType.TSV =>
        saveAsCSV(
          path,
          dataFrame,
          tSVAttributes,
          sourceOperatorInfo,
          addendum
        )
    }
  }

  def saveDataFrameDefault[T <: HdfsStorageFormatType](
                                                        path: String,
                                                        dataFrame: DataFrame,
                                                        sourceOperatorInfo: Option[OperatorInfo]): HdfsTabularDataset = {
    saveDataFrame(path, dataFrame, HdfsStorageFormatType.TSV, overwrite = true,
      sourceOperatorInfo, Map[String, AnyRef](), TSVAttributes.default)
  }


  /**
    * Save a data frame to a path using the given storage format, and return
    * a corresponding HdfsTabularDataset object that points to the path.
    *
    * @param path               The path to which we'll save the data frame.
    * @param dataFrame          The data frame that we want to save.
    * @param storageFormat      The format that we want to store in.
    * @param overwrite          Whether to overwrite any existing file at the path.
    * @param sourceOperatorInfo Mandatory source operator information to be included
    *                           in the output object.
    * @param addendum           Mandatory addendum information to be included in the output
    *                           object.
    * @return After saving the data frame, returns an HdfsTabularDataset object.
    * @deprecated use saveDataFrame(String, dataFrame, HdfsStorageFormatType, Option[OperatorInfo], boolean). or
    *             SaveDataFrameDefault.
    */
  @deprecated("Use signature with HdfsStorageFormatType rather than HdfsStorageFormat enum or saveDataFrameDefault")
  def saveDataFrame(
                     path: String,
                     dataFrame: DataFrame,
                     storageFormat: HdfsStorageFormat.HdfsStorageFormat,
                     overwrite: Boolean,
                     sourceOperatorInfo: Option[OperatorInfo],
                     addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsTabularDataset = {

    deleteOrFailIfExists(path, overwrite)

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
        saveAsCSV(
          path,
          dataFrame,
          TSVAttributes.defaultCSV,
          sourceOperatorInfo,
          addendum
        )
    }
  }

  /**
    * Write a DataFrame to HDFS as a Parquet file, and return an instance of the
    * HDFSParquet IO base type which contains the Alpine 'TabularSchema' definition (created by
    * converting the DataFrame schema) and the path to the saved data.
    */
  def saveAsParquet(path: String,
                    dataFrame: DataFrame,
                    sourceOperatorInfo: Option[OperatorInfo],
                    addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsParquetDataset = {
    val (withDatesChanged, tabularSchema) = dealWithDates(dataFrame)
    withDatesChanged.write.parquet(path)
    new HdfsParquetDatasetDefault(path, tabularSchema, sourceOperatorInfo, addendum)
  }

  /**
    * Write a DataFrame as an HDFSAvro dataset, and return the an instance of the Alpine
    * HDFSAvroDataset type which contains the  'TabularSchema' definition
    * (created by converting the DataFrame schema) and the path to the saved data.
    */
  def saveAsAvro(path: String,
                 dataFrame: DataFrame,
                 sourceOperatorInfo: Option[OperatorInfo],
                 addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsAvroDataset = {
    val (withDatesChanged, tabularSchema) = dealWithDates(dataFrame)
    withDatesChanged.write.format("com.databricks.spark.avro").save(path)
    new HdfsAvroDatasetDefault(path, tabularSchema, sourceOperatorInfo, addendum)
  }

  // ======================================================================
  // Storage util functions for delimited data.
  // ======================================================================


  /**
    * More general version of saveAsCSV.
    * Write a DataFrame to HDFS as a Tabular Delimited file, and return an instance of the Alpine
    * HDFSDelimitedTabularDataset type  which contains the Alpine 'TabularSchema' definition (created by converting
    * the DataFrame schema) and the path to the saved data. Also writes the ".alpine_metadata"
    * to the result directory so that the user can drag and drop the result output and use it without
    * configuring the dataset
    *
    * @param path               where file will be written (this function will create a directory of part files)
    * @param dataFrame          - data to write
    * @param tSVAttributes      - an object which specifies how the file should be written
    * @param sourceOperatorInfo from parameters. Includes name and UUID
    *                           Same as 'saveAsCSV' but also writes the ".alpine_metadata" to the result so  that
    *                           the user can drag and drop the result output and use it without
    *                           configuring the dataset
    */
  def saveAsCSV(path: String, dataFrame: DataFrame,
                tSVAttributes: TSVAttributes,
                sourceOperatorInfo: Option[OperatorInfo],
                addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    val dataset = saveAsCSVoMetadata(path, dataFrame, tSVAttributes, sourceOperatorInfo, addendum)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    SparkMetadataWriter.writeMetadataForDataset(dataset, fileSystem)
    dataset
  }


  /**
    * Write a DataFrame to HDFS as a Tab Delimited file, and return an instance of the Alpine
    * HDFSDelimitedTabularDataset type  which contains the Alpine 'TabularSchema' definition (created by converting
    * the DataFrame schema) and the path to the saved data. Uses the default TSVAttributes object
    * which specifies that the data be written as a Tab Delimited File. See TSVAAttributes for more
    * information and use the saveAsCSV file to customize csv options such as null string and delimiters.
    *
    * Also writes the ".alpine_metadata"
    * to the result directory so that the user can drag and drop the result output and use it without
    * configuring the dataset
    */
  def saveAsTSV(path: String,
                dataFrame: DataFrame,
                sourceOperatorInfo: Option[OperatorInfo],
                addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsDelimitedTabularDataset = {

    val (withDatesChanged, tabularSchema) = dealWithDates(dataFrame)
    withDatesChanged.saveAsCsvFile(
      path,
      Map(
        "delimiter" -> "\t",
        "escape" -> "\\",
        "quote" -> "\"",
        "header" -> "false"
      )
    )

    new HdfsDelimitedTabularDatasetDefault(
      path,
      tabularSchema,
      TSVAttributes.default,
      sourceOperatorInfo,
      addendum
    )
  }

  /**
    * More general version of saveAsTSV.
    * Write a DataFrame to HDFS as a Tabular Delimited file, and return an instance of the Alpine
    * HDFSDelimitedTabularDataset type  which contains the Alpine 'TabularSchema' definition (created by converting
    * the DataFrame schema) and the path to the saved data.
    *
    * @param path               where file will be written (this function will create a directory of part files)
    * @param dataFrame          - data to write
    * @param tSVAttributes      - an object which specifies how the file should be written
    * @param sourceOperatorInfo from parameters. Includes name and UUID
    */
  def saveAsCSVoMetadata(path: String, dataFrame: DataFrame,
                         tSVAttributes: TSVAttributes,
                         sourceOperatorInfo: Option[OperatorInfo],
                         addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    val (withDatesChanged, tabularSchema) = dealWithDates(dataFrame)
    withDatesChanged.saveAsCsvFile(path, tSVAttributes.toMap)
    new HdfsDelimitedTabularDatasetDefault(
      path,
      tabularSchema,
      tSVAttributes,
      sourceOperatorInfo,
      addendum
    )
  }


  /**
    * Checks if the given file path already exists (and would cause a 'PathAlreadyExists'
    * exception when we try to write to it) and deletes the directory to prevent existing
    * results at that path if they do exist.
    *
    * @param outputPathStr - the full HDFS path
    * @return
    */
  def deleteFilePathIfExists(outputPathStr: String) = {
    val outputPath = new Path(outputPathStr)
    if (driverHdfs.exists(outputPath)) {
      println("The path exists already.")
      driverHdfs.delete(outputPath, true)
    }
  }

  @throws[OperatorFailedException]
  def deleteOrFailIfExists[T <: HdfsStorageFormatType](path: String, overwrite: Boolean): Unit = {
    if (overwrite) {
      deleteFilePathIfExists(path)
    } else {
      val outputPath = new Path(path)
      if (driverHdfs.exists(outputPath)) {
        throw new OperatorFailedException(
          "Results file already exists. Set “Overwrite” to “Yes” or change output location (output file: " + path + ")."
        )
      }
    }
  }

  // ======================================================================
  // DataFrame util functions.
  // ======================================================================

  /**
    * Returns a DataFrame from an Alpine HdfsTabularDataset. The DataFrame's schema will
    * correspond to the column header of the Alpine dataset.
    * Uses the databricks csv parser from spark-csv with the following options:
    * 1.withParseMode("DROPMALFORMED"): Catch parse errors such as number format exception caused by a
    * string value in a numeric column and remove those rows rather than fail.
    * 2.withTreatEmptyValuesAsNulls(true) -> the empty string will represent a null value in char columns as it does in alpine
    * 3.If a TSV, The delimiter attributes specified by the TSV attributes object
    * *
    * Date format behavior: DateTime columns are parsed as dates and then converted to the TimeStampType according to
    * the format specified by the Alpine type 'ColumnType' format argument. The original format is save in the schema as metadata for that column.
    * It can be accessed with SparkSqlDateTimeUtils.getDatFormatInfo(structField) for any given column.
    *
    * @param dataset Alpine specific object. Usually input or output of operator.
    * @return Spark SQL DataFrame
    */
  def getDataFrame(dataset: HdfsTabularDataset): DataFrame = {
    val tabularSchema = dataset.tabularSchema
    val schema = convertTabularSchemaToSparkSQLSchema(tabularSchema, keepDatesAsStrings = true)
    val dateFormats = getDateMap(tabularSchema)
    val sqlContext = new SQLContext(sc)
    val path = dataset.path

    val tabularData = dataset match {
      case _: HdfsAvroDataset =>
        sqlContext.read.format("com.databricks.spark.avro").load(path)
      case _: HdfsParquetDataset =>
        sqlContext.read.load(path)
      case _ =>
        val delimitedDataset = dataset.asInstanceOf[HdfsDelimitedTabularDataset]
        val tsvAttributes: TSVAttributes = delimitedDataset.tsvAttributes

        //TODO: Set up mirror with new parser to match behavior of alpine.
        new CsvParser()
          .withParseMode("DROPMALFORMED")
          .withTreatEmptyValuesAsNulls(true)
          .withUseHeader(tsvAttributes.containsHeader)
          .withDelimiter(tsvAttributes.delimiter)
          .withQuoteChar(tsvAttributes.quoteStr)
          .withEscape(tsvAttributes.escapeStr)
          .withSchema(schema)
          .csvFile(sqlContext, path)
    }
    //map dateTime columns from string to java.sql.Date objects. Will appear in schema as TimeStampType objects
    mapDFtoUnixDateTime(tabularData, dateFormats)

  }

  /**
    * For use with hive. Returns a Spark data frame given a hive table.
    */
  def getDataFrame(dataset: HiveTable): DataFrame = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.table(dataset.getConcatenatedName)
  }

  /**
    * STRING -> JAVA TIMESTAMP OBJECT (based on unix time stamp)
    * Take in a dataframe and a map of the column names to the date formats represented in them
    * and use the Spark SQL: "unix_timestamp" UDF,  to change the columns with string dates into
    * unix timestamps in seconds and a custom udf to change that into java dates.
    * Preserves original naming of the columns. Columns which were originally DateTime columns will
    * now be of TimeStampType rather than StringType.
    *
    * @param dataFrame the input dataframe where the date rows are as strings.
    * @param map       columnName -> dateFormat for parsing
    * @throws Exception "Illegal Date Format" if one of the date formats provided is not a valid
    *                   Java SimpleDateFormat pattern.
    *                   And "Could not parse dates correctly. " if the date format is valid, but
    *                   doesn't correspond to the data that is actually in the column.
    */
  def mapDFtoUnixDateTime(dataFrame: DataFrame, map: Map[String, String]): DataFrame = {
    // dataFrame.sqlContext.udf.register("makeDateTime", makeDateTime(_:String,_:String))
    if (map.isEmpty) {
      dataFrame
    }
    else {

      import org.apache.spark.sql.functions.unix_timestamp
      //check that all the date formats are valid Simple Date Formats and throw a reasonable exception if
      //they are not
      try {
        validateDateFormatMap(map)
        //generate a function that maps from the unix time stamp to the java.sql.Date object for a given format

        def dateTimeFunction(format: String): UserDefinedFunction = {
          import org.apache.spark.sql.functions.udf
          udf((time: Long) => new Timestamp(time * 1000))
        }

        import org.apache.spark.sql.functions.{lit, when}

        val selectExpression = dataFrame.schema.fieldNames.map(columnName =>
          map.get(columnName) match {
            case None => dataFrame(columnName)
            case Some(format) =>
              lazy val unixCol = unix_timestamp(dataFrame(columnName), format)
              val nulled = when(unixCol.isNull, lit(null))
                .otherwise(dateTimeFunction(format)(unixCol))
              nulled.cast(TimestampType
              ).as(columnName, dataFrame.schema(columnName).metadata)
          })

        dataFrame.select(selectExpression: _*)
      }
      catch {
        case (e: IllegalArgumentException) => throw new IllegalArgumentException("Failed at read: " + e.getMessage, e)
        case (e: Throwable) => throw new RuntimeException("Failed at read: Could not parse dates correctly. " +
          "Could not read data: Please check that the date formats provided correspond to the data in the columns.", e)
      }
    }
  }

  def validateDateFormatMap(map: Map[String, String]): Unit = {
    map.foreach {
      case (colName, dateFormat) =>
        val simpleDateFormat = Try(new SimpleDateFormat(dateFormat))
        if (simpleDateFormat.isFailure)
          throw new IllegalArgumentException("Date format " + dateFormat + " for column " + colName + " is not a valid SimpleDateFormat",
            simpleDateFormat.failed.get)
    }
  }

  /**
    * JAVA TIME STAMP OBJECT--> STRING
    * Take in a dataFrame and map of the column names to the date formats we want to print and use
    * the Spark SQL UDF date_format to convert from the TimeStamp type to a string representation of the date or time.
    *
    * @param dataFrame input data where date columns are represented as java TimeStamp Objects
    * @param map       columnName -> dateFormat to convert to
    */
  def mapDFtoCustomDateTimeFormat(dataFrame: DataFrame, map: Map[String, String]): DataFrame = {
    if (map.isEmpty) {
      dataFrame
    }
    else {
      try {
        import org.apache.spark.sql.functions.date_format
        validateDateFormatMap(map)

        val selectExpression = dataFrame.schema.fieldNames.map(colDef =>
          map.get(colDef) match {
            case None => dataFrame(colDef)
            case Some(format) =>
              date_format(dataFrame(colDef), format).as(colDef, dataFrame.schema(colDef).metadata)
          })
        dataFrame.select(selectExpression: _*)
      }
      catch {
        case (e: IllegalArgumentException) => throw new IllegalArgumentException("Failed to save results: " + e.getMessage, e)
        case (e: Throwable) =>
          throw new RuntimeException("Failed to save results : Could not convert to custom date formats.", e)
      }
    }
  }

  private def dealWithDates(dataFrame: DataFrame) = {
    val alpineSchema = convertSparkSQLSchemaToTabularSchema(dataFrame.schema)
    val map = getDateMap(alpineSchema)
    (mapDFtoCustomDateTimeFormat(dataFrame, map), alpineSchema)
  }
}

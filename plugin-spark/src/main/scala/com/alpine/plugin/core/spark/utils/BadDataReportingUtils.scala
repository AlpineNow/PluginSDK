package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.io.{TSVAttributes, OperatorInfo}
import com.alpine.plugin.core.utils._
import com.alpine.plugin.core.utils.HdfsStorageFormat.HdfsStorageFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.util.Try

object BadDataReportingUtils {
  val defaultDataRemovedMessage = "due to null values"

  /**
   * Given a dataFrame, the parameters and an instance of sparkRuntimeUtils, filters out all the
   * rows containing null values. Writes those rows to a file according to the values of the 'dataToWriteParam'
   * and the 'badDataPathParam' (provided in the HdfsParameterUtils class). The method returns the data
   * frame which does not contain nulls as well as a string containing an HTML formatted table with
   * the information about the what data was removed and if/where it was stored.
   * The message is generated using the 'AddendumWriter' object in the Plugin Core module.
    *
    * Note about Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
   */
  def filterNullDataAndReport(inputDataFrame: DataFrame,
                              operatorParameters: OperatorParameters,
                              sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterNullDataAndReportGeneral(row => row.anyNull, inputDataFrame, operatorParameters, sparkRuntimeUtils, defaultDataRemovedMessage)
  }

  @deprecated("use filterNullDataAndReport")
  def filterBadDataAndReport(inputDataFrame: DataFrame,
                             operatorParameters: OperatorParameters,
                             sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterNullDataAndReportGeneral(row => row.anyNull, inputDataFrame, operatorParameters, sparkRuntimeUtils, defaultDataRemovedMessage)
  }

  /**
    * Same as 'filterNullDataAndReport' but rather than using the .anyNull function in the dataFrame class allows the user
    * to define a function which returns a boolean for each row is it contains data which should be removed.
    *
    * * Note about Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
   */
  def filterNullDataAndReportGeneral(removeRow: Row => Boolean, inputDataFrame: DataFrame,
                                     operatorParameters: OperatorParameters,
                                     sparkRuntimeUtils: SparkRuntimeUtils, dataRemovedDueTo: String): (DataFrame, String) = {
    val operatorInfo = Some(operatorParameters.operatorInfo)
    val dataToWriteParam = HdfsParameterUtils.getAmountOfBadDataToWrite(operatorParameters)
    val badDataPath = HdfsParameterUtils.getBadDataPath(operatorParameters)
    val hdfsStorageFormat = Try(HdfsParameterUtils.getHdfsStorageFormatType(operatorParameters)
    ).getOrElse(HdfsStorageFormatType.TSV)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)
    val (goodDataFrame, badDataFrame) = removeDataFromDataFrame(removeRow, inputDataFrame, dataToWriteParam)
    val inputCount = inputDataFrame.count()
    val outputCount = goodDataFrame.count()
    val message = handleNullDataAsDataFrame(dataToWriteParam, badDataPath, inputCount, outputCount
      , badData = badDataFrame, sparkRuntimeUtils, hdfsStorageFormat,
      overwrite, operatorInfo, dataRemovedDueTo)
    (goodDataFrame, message)
  }

  @deprecated("Use filterNullDataAndReportGeneral")
  def filterBadDataAndReportGeneral(isBad: Row => Boolean, inputDataFrame: DataFrame,
                                    operatorParameters: OperatorParameters,
                                    sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterNullDataAndReportGeneral(isBad, inputDataFrame, operatorParameters, sparkRuntimeUtils, defaultDataRemovedMessage)
  }

  /**
    * Rather than filtering the data, just provide an RDD of Strings that contain the null data and
   * write the data and report according to the values of the other parameters.
    *
    * * Note about Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
   */
  def reportNullDataAsStringRDD(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                inputDataSize: Long, outputSize: Long, badData: Option[RDD[String]],
                                dataRemovedDueTo: String
                                ): String = {
    val badDataAsDF = badData match {
      case Some(rdd) =>
        val sqlContext = new SQLContext(rdd.sparkContext)
        val dummySchema = StructType(Array(StructField("String", StringType, nullable = true)))
        Some(sqlContext.createDataFrame(rdd.map(r => Row.fromSeq(Seq(r))), dummySchema))
      case None => None
    }
    val (dataToWrite, message) = getNullDataToWriteMessage(amountOfDataToWriteParam, badDataPath,
      inputDataSize, outputSize, badDataAsDF, dataRemovedDueTo)
    if (dataToWrite.nonEmpty) dataToWrite.get.rdd.map(_.get(0).toString).saveAsTextFile(badDataPath)
    message
  }

  @deprecated("Use reportNullDataAsStringRDD ")
  def reportBadDataAsStringRDD(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                               inputDataSize: Long, outputSize: Long, badData: Option[RDD[String]]
                              ): String = {
    reportNullDataAsStringRDD(amountOfDataToWriteParam, badDataPath, inputDataSize, outputSize, badData, defaultDataRemovedMessage)
  }


  //TODO: It would be awesome if we could write the meta data for the bad data, so that it would be easy to do some analysis of it.

  /**
    *
    * @deprecated  Use the method with the signature that includes a value of type
    *              HDFSStorageFormatType rather than the HdfsStorage format enum. Or handleBadDataAsDataFrameDefault
   */
  @deprecated("Use signature with HdfsStorageFormatType or handleBadDataAsDataFrameDefault")
  def handleBadDataAsDataFrame(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                               inputDataSize: Long,
                               outputSize: Long, badData: Option[DataFrame],
                               sparkRuntimeUtils: SparkRuntimeUtils,
                               hdfsStorageFormat: HdfsStorageFormat = HdfsStorageFormat.TSV,
                               overwrite: Boolean = true,
                               operatorInfo: Option[OperatorInfo] = None): String = {
    val (dataToWrite, message) = getNullDataToWriteMessage(amountOfDataToWriteParam, badDataPath,
      inputDataSize, outputSize, badData, defaultDataRemovedMessage)
    //save the dataToWrite
    if (dataToWrite.nonEmpty) {
      val df: DataFrame = dataToWrite.get
      sparkRuntimeUtils.saveDataFrame(badDataPath, df, HdfsStorageFormat.TSV, overwrite, operatorInfo)
    }
    message
  }

  /**
    * If specified by Params will write data containing null values to a file. Regardless return a message about how much data was removed.
    */
  def handleNullDataAsDataFrame[T <: HdfsStorageFormatType](amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                                            inputDataSize: Long,
                                                            outputSize: Long, badData: Option[DataFrame],
                                                            sparkRuntimeUtils: SparkRuntimeUtils,
                                                            hdfsStorageFormatType: T,
                                                            overwrite: Boolean,
                                                            operatorInfo: Option[OperatorInfo], dataRemovedDueTo: String): String = {
    val (dataToWrite, message) = getNullDataToWriteMessage(amountOfDataToWriteParam, badDataPath,
      inputDataSize, outputSize, badData, dataRemovedDueTo)
    //save the dataToWrite
    if (dataToWrite.nonEmpty) {
      val df: DataFrame = dataToWrite.get
      sparkRuntimeUtils.saveDataFrame(badDataPath, df, hdfsStorageFormatType, overwrite,
        operatorInfo, Map[String, AnyRef](), TSVAttributes.default)
    }
    message
  }

  /**
    * If applicable writes bad data as a TSV with default attributes.
    * @return
    */
  def handleNullDataAsDataFrameDefault[T <: HdfsStorageFormatType](amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                                                   inputDataSize: Long,
                                                                   outputSize: Long, nullData: Option[DataFrame],
                                                                   sparkRuntimeUtils: SparkRuntimeUtils, dataRemovedDueTo: String = defaultDataRemovedMessage): String = {
    handleNullDataAsDataFrame(amountOfDataToWriteParam, badDataPath, inputDataSize, outputSize,
      nullData, sparkRuntimeUtils, HdfsStorageFormatType.TSV, overwrite = true, None, dataRemovedDueTo)
  }
  /**
   * Helper function which uses the AddendumWriter object to generate a message about the bad data and
   * get the data, if any, to write to the bad data file.
    * The data removed parameter is the message for what the bad data was removed. It will be of the form
    * "data removed " + dataRemovedDueTo. I.e. if you put "due to zero values" then the message would read
    * "Data removed due to zero values".
   */
  def getNullDataToWriteMessage(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                inputDataSize: Long,
                                outputSize: Long, badData: Option[DataFrame], dataRemovedDueTo: String): (Option[DataFrame], String) = {
    (amountOfDataToWriteParam, badData) match {
      case (Some(n), Some(data)) =>
        val badDataSize = inputDataSize - outputSize
        val (dataToWrite, locationMsg) =
          if (badDataSize == 0) (None, "<br>No data removed " + dataRemovedDueTo + "<br>")

          else if (n < Long.MaxValue && n < badDataSize) {
            val sampleSize: Double = n / badDataSize.toDouble
            val sampledData = data.sample(withReplacement = false, sampleSize)
            (Some(sampledData), " <br> A sample of the " + n + " rows of data removed (" + dataRemovedDueTo +
              ") has been written to file: <br>" + badDataPath + "<br>")
          }
          else
            (Some(data), "<br>All the data removed (" + dataRemovedDueTo + ") has been written to file: <br>" + badDataPath + "<br>")

        val lists = AddendumWriter.generateBadDataReport(inputDataSize, outputSize)
        val table = HtmlTabulator.format(lists)
        (dataToWrite, table + locationMsg)
      case (_, _) => val lists = AddendumWriter.generateBadDataReport(inputDataSize, outputSize)
        (None, HtmlTabulator.format(lists))
    }
  }

  @deprecated("Use getNullDataToWriteMessage")
  def getBadDataToWriteAndMessage(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                  inputDataSize: Long,
                                  outputSize: Long, badData: Option[DataFrame]): (Option[DataFrame], String) = {
    getNullDataToWriteMessage(amountOfDataToWriteParam, badDataPath, inputDataSize, outputSize, badData, defaultDataRemovedMessage)
  }

  private def createNullableSchema(schema: StructType): StructType = {
    StructType(schema.map(field => StructField(field.name, field.dataType, nullable = true)))
  }

  /**
   * Split a DataFrame according to the value of the rowIsBad parameter.
    *
    * Note about Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
   */
  def removeDataFromDataFrame(removeRow: Row => Boolean, inputDataFrame: DataFrame,
                              dataToWriteParam: Option[Long] = Some(Long.MaxValue)
                               ): (DataFrame, Option[DataFrame]) = {
    //tag each row as true, if it contains null, or false otherwise
    val schema = inputDataFrame.schema
    val sqlContext = inputDataFrame.sqlContext
    val tagInputData: RDD[(Boolean, Row)] =
      inputDataFrame.map(row => (removeRow(row), row))
    val badDataFrame = dataToWriteParam match {
      case Some(n) =>
        val badRowRDD = tagInputData.filter(_._1).values
        val nullableSchema = createNullableSchema(schema)
        Some(sqlContext.createDataFrame(badRowRDD, nullableSchema))
      case None => None
    }
    val goodRowRDD = tagInputData.filter(!_._1).values
    val goodDataFrame = sqlContext.createDataFrame(goodRowRDD, schema)
    (goodDataFrame, badDataFrame)
  }
}

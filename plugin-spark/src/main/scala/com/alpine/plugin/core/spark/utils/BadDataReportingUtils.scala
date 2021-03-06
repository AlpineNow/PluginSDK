package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.io.{TSVAttributes, OperatorInfo}
import com.alpine.plugin.core.utils._
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
    * Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
    */
  def filterNullDataAndReport(inputDataFrame: DataFrame,
                              operatorParameters: OperatorParameters,
                              sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterNullDataAndReportGeneral(row => row.anyNull, inputDataFrame, operatorParameters,
      sparkRuntimeUtils, defaultDataRemovedMessage)
  }

  @deprecated("use filterNullDataAndReport")
  def filterBadDataAndReport(inputDataFrame: DataFrame,
                             operatorParameters: OperatorParameters,
                             sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterNullDataAndReportGeneral(row => row.anyNull, inputDataFrame, operatorParameters,
      sparkRuntimeUtils, defaultDataRemovedMessage)
  }

  /**
    * Same as 'filterNullDataAndReport' but rather than using the .anyNull function in the dataFrame class allows the user
    * to define a function which returns a boolean for each row is it contains data which should be removed.
    *
    * Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
    */
  def filterNullDataAndReportGeneral(removeRow: Row => Boolean, inputDataFrame: DataFrame,
                                     operatorParameters: OperatorParameters,
                                     sparkRuntimeUtils: SparkRuntimeUtils, dataRemovedDueTo: String): (DataFrame, String) = {
    val operatorInfo = Some(operatorParameters.operatorInfo)
    val dataToWriteParam = HdfsParameterUtils.getAmountOfBadDataToWrite(operatorParameters)
    val badDataPath = HdfsParameterUtils.getBadDataPath(operatorParameters)
    val hdfsStorageFormat = Try(HdfsParameterUtils.getHdfsStorageFormatType(operatorParameters)).getOrElse(HdfsStorageFormatType.CSV)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)
    val (goodDataFrame, badDataFrame) = removeDataFromDataFrame(removeRow, inputDataFrame, dataToWriteParam)
    val countRows = HdfsParameterUtils.countRowsRemovedDueToNullData(operatorParameters)
    val message = if (countRows) {
      val ar = inputDataFrame.rdd.treeAggregate(Array(0, 0))(
        seqOp = (ar, row) =>
          if (removeRow(row)) {
            ar(1) += 1 //bad data count is at index 1
            ar
          } else {
            ar(0) += 1 //good data is at index 2
            ar
          },
        combOp = (v1, v2) => {
          v1(0) += v2(0)
          v1(1) += v2(1)
          v1
        })
      val badDataCount = ar(1)
      val outputCount = ar(0)
      val inputCount = badDataCount + outputCount

      handleNullDataAsDataFrame(dataToWriteParam, badDataPath, inputCount, outputCount
        , badData = badDataFrame, sparkRuntimeUtils, hdfsStorageFormat,
        overwrite, operatorInfo, dataRemovedDueTo)
    } else {
      //In this case don't count the rows removed at all, to avoid that extra shuffle.
      "Some rows of data may have been removed " + dataRemovedDueTo +
        ".You have selected not to count the number of rows removed to speed up the computation of the operator."
    }
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
    * Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
    */
  def reportNullDataAsStringRDD(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                inputDataSize: Long, outputSize: Long, badData: Option[RDD[String]],
                                dataRemovedDueTo: String): String = {
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



  /**
    * If specified by Params will write data containing null values to a file. Regardless return a message
    * about how much data was removed.
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
        Map[String, AnyRef](), TSVAttributes.defaultCSV, HdfsCompressionType.NoCompression)
    }
    message
  }

  /**
    * If applicable writes bad data as a CSV with default attributes.
    *
    * @return
    */
  def handleNullDataAsDataFrameDefault[T <: HdfsStorageFormatType](amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                                                   inputDataSize: Long,
                                                                   outputSize: Long, nullData: Option[DataFrame],
                                                                   sparkRuntimeUtils: SparkRuntimeUtils, dataRemovedDueTo: String = defaultDataRemovedMessage): String = {
    handleNullDataAsDataFrame(amountOfDataToWriteParam, badDataPath, inputDataSize, outputSize,
      nullData, sparkRuntimeUtils, HdfsStorageFormatType.CSV, overwrite = true, None, dataRemovedDueTo)
  }

  /**
    * Helper function which uses the AddendumWriter object to generate a message about the bad data and* get the data, if any, to write to the bad data file.
    * The data removed parameter is the message for what the bad data was removed. It will be of the form
    * "data removed " + dataRemovedDueTo. I.e. if you put "due to zero values" then the message would read
    * "Data removed due to zero values".
    *
    * Note: This method should NOT be called in the event that the user selected the
    * "Do Not Count # of Rows Removed (Faster)" option.
    */
  def getNullDataToWriteMessage(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                inputDataSize: Long,
                                outputSize: Long, badData: Option[DataFrame], dataRemovedDueTo: String): (Option[DataFrame], String) = {
    (amountOfDataToWriteParam, badData) match {
      case (Some(n), Some(data)) =>
        val badDataSize = inputDataSize - outputSize
        val (dataToWrite, locationMsg) =
          if (badDataSize == 0) (None, "")

          else if (n < Long.MaxValue && n < badDataSize) {
            val sampleSize: Double = n / badDataSize.toDouble
            val sampledData = data.sample(withReplacement = false, sampleSize)
            (Some(sampledData), " <br> A sample of the " + n + " rows of data removed (" + dataRemovedDueTo +
              ") has been written to file: <br>" + badDataPath + "<br>")
          }
          else
            (Some(data), "<br>All the data removed (" + dataRemovedDueTo + ") has been written to file: <br>" + badDataPath + "<br>")

        val lists = AddendumWriter.generateNullDataReport(inputDataSize, outputSize, dataRemovedDueTo)
        val table = HtmlTabulator.format(lists)
        (dataToWrite, table + locationMsg)
      case (_, _) => val lists = AddendumWriter.generateNullDataReport(inputDataSize, outputSize, dataRemovedDueTo)
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
    * Split a DataFrame according to the value of the removeRow parameter.
    *
    * Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns). We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.
    *
    * @param removeRow        A function from spark.sql.Row to boolean. Should return true if the row is
    *                         false.
    * @param inputDataFrame   Input data read without null or bad data removed.
    * @param dataToWriteParam None if write no data. Some(n) if parameter value is write n rows.
    * @return
    */
  def removeDataFromDataFrame(removeRow: Row => Boolean, inputDataFrame: DataFrame,
                              dataToWriteParam: Option[Long] = Some(Long.MaxValue)
                             ): (DataFrame, Option[DataFrame]) = {
    //tag each row as true, if it contains null, or false otherwise
    val schema = inputDataFrame.schema
    val sqlContext = inputDataFrame.sqlContext
    val tagInputData: RDD[(Boolean, Row)] =
      inputDataFrame.rdd.map(row => (removeRow(row), row))
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

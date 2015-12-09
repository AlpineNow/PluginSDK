package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.io.OperatorInfo
import com.alpine.plugin.core.utils.HdfsStorageFormat.HdfsStorageFormat
import com.alpine.plugin.core.utils.{AddendumWriter, HdfsParameterUtils, HdfsStorageFormat, HtmlTabulator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object BadDataReportingUtils {

  /**
   * Given a dataFrame, the parameters and an instance of sparkRuntimeUtils, filters out all the
   * rows containing null values. Writes those rows to a file according to the values of the 'dataToWriteParam'
   * and the 'badDataPathParam' (provided in the HdfsParameterUtils class). The method returns the data
   * frame which does not contain nulls as well as a string containing an HTML formatted table with
   * the information about the what data was removed and if/where it was stored.
   * The message is generated using the 'AddendumWriter' object in the Plugin Core module.
   */
  def filterBadDataAndReport(inputDataFrame: DataFrame,
                             operatorParameters: OperatorParameters,
                             sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    filterBadDataAndReportGeneral(row => row.anyNull, inputDataFrame, operatorParameters, sparkRuntimeUtils)
  }

  /**
   * Same as 'filterBadDataAndReport' but rather than using the .anyNull function in the dataFrame class allows the user
   * to define a function which returns a boolean for each row is it contains bad data.
   */
  def filterBadDataAndReportGeneral(isBad: Row => Boolean, inputDataFrame: DataFrame,
                                    operatorParameters: OperatorParameters,
                                    sparkRuntimeUtils: SparkRuntimeUtils): (DataFrame, String) = {
    val operatorInfo = Some(operatorParameters.operatorInfo)
    val dataToWriteParam = HdfsParameterUtils.getAmountOfBadDataToWrite(operatorParameters)
    val badDataPath = HdfsParameterUtils.getBadDataPath(operatorParameters)
    val hdfsStorageFormat = HdfsParameterUtils.getHdfsStorageFormat(operatorParameters)
    val overwrite = HdfsParameterUtils.getOverwriteParameterValue(operatorParameters)
    val (goodDataFrame, badDataFrame) = removeDataFromDataFrame(isBad, inputDataFrame, dataToWriteParam)
    val message = handleBadDataAsDataFrame(dataToWriteParam, badDataPath, inputDataFrame.count(),
      goodDataFrame.count(), badData = badDataFrame, sparkRuntimeUtils, hdfsStorageFormat,
      overwrite, operatorInfo)
    (goodDataFrame, message)
  }

  /**
   * Rather than filtering the data, just provide an RDD of Strings that contain the bad data and
   * write the data and report according to the values of the other parameters.
   */
  def reportBadDataAsStringRDD(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                               inputDataSize: Long, outputSize: Long, badData: Option[RDD[String]]
                                ): String = {
    val badDataAsDF = badData match {
      case Some(rdd) =>
        val sqlContext = new SQLContext(rdd.sparkContext)
        val dummySchema = StructType(Array(StructField("String", StringType, true)))
        Some(sqlContext.createDataFrame(rdd.map(r => Row.fromSeq(Seq(r))), dummySchema))
      case None => None
    }
    val (dataToWrite, message) = getBadDataToWriteAndMessage(amountOfDataToWriteParam, badDataPath,
      inputDataSize, outputSize, badDataAsDF)
    if (dataToWrite.nonEmpty) dataToWrite.get.rdd.map(_.get(0).toString).saveAsTextFile(badDataPath)
    message
  }

  //toDo: It would be awesome if we could write the meta data for the bad data, so that it would be easy to do some analysis of it.

  /**
   * Rather than filtering a DataFrame, use this method if you already have the bad data as a data frame.
   */
  def handleBadDataAsDataFrame(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                               inputDataSize: Long,
                               outputSize: Long, badData: Option[DataFrame],
                               sparkRuntimeUtils: SparkRuntimeUtils,
                               hdfsStorageFormat: HdfsStorageFormat = HdfsStorageFormat.TSV,
                               overwrite: Boolean = true,
                               operatorInfo: Option[OperatorInfo] = None): String = {
    val (dataToWrite, message) = getBadDataToWriteAndMessage(amountOfDataToWriteParam, badDataPath,
      inputDataSize, outputSize, badData)
    //save the dataToWrite
    if (dataToWrite.nonEmpty) {
      val df: DataFrame = dataToWrite.get
      sparkRuntimeUtils.saveDataFrame(badDataPath, df, HdfsStorageFormat.TSV, overwrite, operatorInfo)
    }
    message
  }

  /**
   * Helper function which uses the AddendumWriter object to generate a message about the bad data and
   * get the data, if any, to write to the bad data file.
   */
  def getBadDataToWriteAndMessage(amountOfDataToWriteParam: Option[Long], badDataPath: String,
                                  inputDataSize: Long,
                                  outputSize: Long, badData: Option[DataFrame]): (Option[DataFrame], String) = {
    (amountOfDataToWriteParam, badData) match {
      case (Some(n), Some(data)) =>
        val badDataSize = inputDataSize - outputSize
        val (dataToWrite, locationMsg) =
          if (n < Long.MaxValue && n < badDataSize) {
            val sampleSize: Double = n / badDataSize.toDouble
            val sampledData = data.sample(withReplacement = false, sampleSize)
            (sampledData, " <br> A sample of " + n + " rows of data written to file: <br>" + badDataPath)
          }
          else
            (data, "<br> All bad data written to file: <br>" + badDataPath)

        val lists = AddendumWriter.generateBadDataReport(inputDataSize, outputSize)
        val table = HtmlTabulator.format(lists)
        (Some(dataToWrite), table + locationMsg)
      case (_, _) => val lists = AddendumWriter.generateBadDataReport(inputDataSize, outputSize)
        (None, HtmlTabulator.format(lists))
    }
  }

  private def createNullableSchema(schema: StructType): StructType = {
    StructType(schema.map(field => StructField(field.name, field.dataType, nullable = true)))
  }

  /**
   * Split a DataFrame according to the value of the rowIsBad parameter.
   */
  def removeDataFromDataFrame(rowIsBad: Row => Boolean, inputDataFrame: DataFrame,
                              dataToWriteParam: Option[Long] = Some(Long.MaxValue)
                               ): (DataFrame, Option[DataFrame]) = {
    //tag each row as true, if it contains null, or false otherwise
    val schema = inputDataFrame.schema
    val sqlContext = inputDataFrame.sqlContext
    val tagInputData: RDD[(Boolean, Row)] =
      inputDataFrame.map(row => (rowIsBad(row), row))
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

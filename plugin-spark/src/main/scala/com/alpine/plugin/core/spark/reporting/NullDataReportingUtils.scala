package com.alpine.plugin.core.spark.reporting

import com.alpine.plugin.core.io.TSVAttributes
import com.alpine.plugin.core.spark.utils.SparkRuntimeUtils
import com.alpine.plugin.core.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


/**
  * Based on Data frames, so they should be faster
  * Use newer language
  */
object NullDataReportingUtils {
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
    *
    */
  def filterNullDataAndReport[T<: NullDataReportingStrategy](inputDataFrame: DataFrame,
                                                             sparkRuntimeUtils: SparkRuntimeUtils,
                                                             nullDataReportingStrategy: T): (DataFrame, String) = {
    nullDataReportingStrategy match {
      case _ : DoNotCount =>
      //Slightly faster not to filter, but to use na.drop which drops all null values
      (inputDataFrame.na.drop(), noCountMessage(defaultDataRemovedMessage))
      case _ =>
        val removeAllColumnExpression = getRemoveAllNullsExpression(inputDataFrame)
        filterNullDataAndReportGeneral(inputDataFrame, sparkRuntimeUtils, nullDataReportingStrategy,
          removeAllColumnExpression, defaultDataRemovedMessage)
    }
  }

  /**
    * Same as 'filterNullDataAndReport' but rather than using the .anyNull function in the dataFrame class allows the user
    * to define a function which returns a boolean for each row is it contains data which should be removed.
    *
    * Dirty Data: Spark SQL cannot process CSV files with dirty data (i.e. String values
    * in numeric columns. We use the Drop Malformed option, so in the case of dirty data, the operator will not
    * fail, but will silently remove those rows.

    * @param inputDataFrame could be a processed version of input data or data comming directly from operator
    * @param sparkRuntimeUtils for writing the null data if necessary
    * @param nullDataReportingStrategy - Either DoNotCount(), DoNotWrite() or WriteAll(path) contains information about
    *                                  whether and where to wrtie/ count null data
    * @param removeCondition - a column condition. Data will be kept if the condition is NOT true, and removed if this condition is true.
    *               For example if the condition is col("dependent").isNull then we will remove all data that is null in the "dependent column.
    *               This expression must be nullable. It will fail if you use "col("dependent").isNull.or(col("dependent").equals(0).
    *               Because the expression !("col("dependent").isNull.or(col("dependent").equals(0)) fails on null values since both if conditions
    *               will be evaluated. Use instead !("col("dependent").isNull.or(col("dependent").eqNullSafe(0))
    * @param dataRemovedDueTo
    * @tparam T
    * @return
    */
  def filterNullDataAndReportGeneral[T<: NullDataReportingStrategy](
                                                                    inputDataFrame: DataFrame,
                                                                    sparkRuntimeUtils: SparkRuntimeUtils,
                                                                    nullDataReportingStrategy: T,
                                                                    removeCondition : Column,
                                                                    dataRemovedDueTo: String
                                                                   ): (DataFrame, String) = {

    val message: String = nullDataReportingStrategy match {
      case WriteAndCount(path, storageFormatType, overwrite) =>
        val (goodCount, nullCount) = countBadData(inputDataFrame, removeCondition)
        val countMessage = HtmlTabulator.format(AddendumWriter.generateNullDataReport(goodCount + nullCount,
          goodCount, dataRemovedDueTo))

        val nullsMessage: String = if(nullCount > 0){
          val badDF = inputDataFrame.filter(removeCondition)
          sparkRuntimeUtils.saveDataFrame(path, badDF, storageFormatType, overwrite,
              Map[String, AnyRef](), TSVAttributes.defaultCSV)
          getNullDataToWriteMessage(path, dataRemovedDueTo)
        }
        else "" //TODO: Something else here?
        countMessage ++ nullsMessage

      case  _ : DoNotWrite =>
        val (goodCount, nullCount) = countBadData(inputDataFrame, removeCondition)
        HtmlTabulator.format(AddendumWriter.generateNullDataReport(goodCount + nullCount, goodCount, dataRemovedDueTo))
      case _ => noCountMessage(dataRemovedDueTo)
    }

    val goodDataFrame = inputDataFrame.filter(!removeCondition)
    (goodDataFrame, message)
  }


  def getRemoveAllNullsExpression(inputDataFrame : DataFrame) : Column =
    isnull(concat(inputDataFrame.columns.map(x => inputDataFrame.col(x)) :_*))


  def noCountMessage(dataRemovedDueTo : String = "Null Values") : String =
    "Some rows of data may have been removed " + dataRemovedDueTo +
    ".You have selected not to count the number of rows removed to speed up the computation of the operator."

  /**
    * returns a count of rows where "expression" is false, and true.
    * @param selected - dataframe to count
    * @param removeCondition - remove condition, must be possible to negate. See "filterNullDataAndReportGeneral" for
    *                        limitations on the remove condition.
    * @return (goodCount, nullCount) where goodCount is the number of rows for which "removeCondition" is false and
    *         null count is the number of rows where it was true.
    */
  def countBadData(selected : DataFrame, removeCondition : Column):(Long, Long) = {

    val aggregated = selected.groupBy(removeCondition).count().collect()

    if (aggregated.length == 2) {
      if(aggregated.exists(_.anyNull)){
        throw new Exception("The remove condition used in NullDataReportingUtils.scala returns null for some" +
                                    " rows in this data frame. Please confirm that the remove condition will return " +
                                    "true or false on all rows even when negated.")
      }
      val mapped = aggregated.sortBy(_.getBoolean(0)).map(_.getLong(1))
      (mapped(0), mapped(1))
    } else {
      val first = aggregated.head
      val (isBad, count) = (first.getBoolean(0), first.getLong(1))
      if (isBad) ( 0L, count)
      else (count, 0L)
    }
  }

  /**
    * Return the text of the message about writing null data to file. Method does not actually filter or write.
    */
  def getNullDataToWriteMessage(
                                 badDataPath: String,
                                 dataRemovedDueTo: String): String = {
    "<br>All the data removed (" + dataRemovedDueTo + ") has been written to file: <br>" + badDataPath + "<br>"
  }
}



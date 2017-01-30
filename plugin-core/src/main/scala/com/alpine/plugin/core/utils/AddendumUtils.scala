package com.alpine.plugin.core.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.concurrent.TimeUnit

import com.alpine.plugin.core.io.{IOBase, TabularDataset}
import com.alpine.plugin.core.visualization.{CompositeVisualModel, HtmlVisualModel, VisualModel, VisualModelFactory}

/**
  * Utilities for writing the addendum for custom operators in a consistent manner
  */
object AddendumWriter {

  val SUMMARY_DISPLAY_NAME = "Summary"
  val OUTPUT_DISPLAY_NAME = "Output"

  val summaryKey = "summaryKey"

  def reportInputSize(n: Long): List[String] = List("Input data size: ", n + " rows")

  def reportOutputSize(n: Long): List[String] = List("Input size after removing rows with null values " + ": ", n + " rows")

  def reportOutputSize(n: Long, reason: String): List[String] =
    List("Input size after removing rows " + reason + ": ", n + " rows")

  def reportBadDataSize(input: Long, output: Long): List[String] = {
    reportNullDataSize(input, output, "due to null values")
  }

  def reportNullDataSize(input: Long, output: Long, reason: String): List[String] = {
    val badRows = input - output
    val badPercent = ((badRows / input.toDouble) * 100).round
    if (badRows == 0)
      List("No data removed " + reason, "")
    else
      List("Rows removed " + reason + ": ", badRows + " rows (" + badPercent + "%)")
  }

  /**
    * Given the input size and output size, generates a report as a list of lists (which can
    * then be formatted as an HTML table with the tabulator class about how much bad data was
    * removed. If, for example, the input had 100 rows and the output had ninety the report would
    * read:
    * Input data size:  100 rows
    * Input data size after removing rows with null values: 90 rows
    * Rows removed (due to null values) : 10 rows (10%)
    */
  def generateBadDataReport(inputSize: Long, outputSize: Long) = List(reportInputSize(inputSize),
    reportOutputSize(outputSize), reportBadDataSize(inputSize, outputSize))

  /**
    * Given the input size and output size, generates a report as a list of lists (which can
    * then be formatted as an HTML table with the tabulator class about how much bad data was
    * removed. If, for example, the input had 100 rows and the output had ninety the report would
    * read:
    * Input data size:  100 rows
    * Input data size after removing rows + REASON + : 90 rows
    * Rows removed (REASON) : 10 rows (10%)
    *
    * @param inputSize
    * @param outputSize
    * @param reason - why the data was removed e.g. "due to null values"
    * @return
    */
  def generateNullDataReport(inputSize: Long, outputSize: Long, reason: String) = List(reportInputSize(inputSize),
    reportOutputSize(outputSize, reason), reportNullDataSize(inputSize, outputSize, reason))

  /**
    * Returns a string describing the name of the results and where they are stored.
    * Has html <br> tags afterwards.
    */
  def reportOutputLocation(outputPath: String, resultMessage: String =
  "The output of the operator is stored at"): String = resultMessage + "<br>" + outputPath + "<br>"

  /**
    * Add to a list that will be used by Tabulator. It creates an empty row in the table
    */
  val emptyRow = List("&nbsp", "&nbsp")

  /**
    * Create a map which can be used as the addendum with one (key, value) pair added.
    * Add more values to the map with map.updated(newKey, newValue)
    *
    * @param summaryText The text which should appear in the summary tab of the result output.
    * @return
    */
  def createStandardAddendum(summaryText: String): Map[String, AnyRef] = {
    Map[String, AnyRef](summaryKey -> summaryText)
  }

  /**
    * Use in the 'OnOutputVisualization class. Creates a composite visualization with
    * - the output of a tabular dataset
    * - the visual models provided by the optional additionalVisualModels parameter
    * - an HtmlVisualModel of the summary if it has been added to the addendum (nothing will be
    * - added if the addendum doesn't include anything with the visual key 'summaryKey'
    */
  def createCompositeVisualModel[O <: TabularDataset](visualModelFactory: VisualModelFactory,
                                                      outputData: O, additionalVisualModels: Array[(String, VisualModel)] = Array.empty[(String, VisualModel)]
                                                     ): CompositeVisualModel = {

    val model = new CompositeVisualModel
    model.addVisualModel(OUTPUT_DISPLAY_NAME, visualModelFactory.createTabularDatasetVisualization(outputData))
    additionalVisualModels.foldLeft(model)(
      (compositeModel, visualModelPair) => {
        compositeModel.addVisualModel(visualModelPair._1, visualModelPair._2)
        compositeModel
      })

    val summaryText = outputData.asInstanceOf[IOBase].addendum.get(summaryKey)

    summaryText match {
      case Some(text) =>
        val summaryVisualModel = HtmlVisualModel(text.toString)
        model.addVisualModel(SUMMARY_DISPLAY_NAME, summaryVisualModel)
        model
      case None => model
    }
  }
}

class Timer() {
  var startTime: Option[Date] = None
  var endTime: Option[Date] = None
  var totalTime: Option[Long] = None
  val DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  def start: this.type = {
    val calender = Calendar.getInstance().getTime
    startTime = Some(calender)
    this
  }

  def stop: this.type = {
    startTime match {
      case (Some(startDate)) =>
        val endDate = Calendar.getInstance().getTime
        endTime = Some(endDate)
        totalTime = Some(endDate.getTime - startDate.getTime)
        this
      case (_) => throw new Exception("Timer never started")
    }
  }

  /**
    * Given an array of java TimeUnits
    * Converts a long representing milli seconds to an array of number of each of the given TimeUnits
    * which should be used to report it. I.e.
    * convertMilliesToOtherUnits(61,000, Array(TimeUnit.MINUTES< TImeUnit.SECONDS)) would return Array(1, 1)
    */
  def convertMilliesToOtherUnits(millies: Long, allTimeUnits: Array[TimeUnit]): Array[Long] = {
    var milliesInRest: Long = millies
    val timeMap = Array.ofDim[Long](allTimeUnits.length)
    var i = 0
    while (i < allTimeUnits.length) {
      val timeUnit = allTimeUnits(i)
      val n = timeUnit.convert(milliesInRest, TimeUnit.MILLISECONDS)
      val diffInMillis = timeUnit.toMillis(n)
      milliesInRest = milliesInRest - diffInMillis
      timeMap.update(i, n)
      i += 1
    }
    timeMap
  }

  /**
    * Reports the start time of a timer, the end time, and the duration in hours, minutes, and seconds.
    */
  def report(dataFormat: SimpleDateFormat): List[List[String]] =
    totalTime match {
      case (None) => List(List("Timer never started"))
      case Some(diffInMillies) =>
        val reportStartTime = dataFormat.format(startTime.get)
        val reportEndTime = dataFormat.format(endTime.get)
        val allTimeUnits = Array(TimeUnit.HOURS, TimeUnit.MINUTES,
          TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
        val timeMap = convertMilliesToOtherUnits(diffInMillies, allTimeUnits)
        val reportingTime = timeMap.mkString(":")
        List(
          List("Time to run: ", reportingTime + " "),
          List(" ", "Start time: " + reportStartTime),
          List(" ", "End time: " + reportEndTime)
        )
    }

  def report(): List[List[String]] = this.report(DEFAULT_DATE_FORMAT)

}



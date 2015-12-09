package com.alpine.plugin.core.utils

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}
import java.util.concurrent.TimeUnit

/**
 * Utilities for writing the addendum for custom operators in a consistent manner
 */
object AddendumWriter {

  def reportInputSize(n: Long): List[String] = List("Input data size: ", n +
    " rows")

  def reportOutputSize(n: Long): List[String] = List("Output data size: ", n +
    " rows")

  def reportBadDataSize(input: Long, output: Long): List[String] = {
    val badRows = input - output
    val badPercent = ((badRows / input.toDouble) * 100).round
    val badRowMsg = if (badRows == 0) "No bad data"
    else
      badRows + " rows (" + badPercent + "%)"
    List("Rows removed due to bad data: ", badRowMsg)
  }

  /**
   * Given the input size and output size, generates a report as a list of lists (which can
   * then be formatted as an HTML table with the tabulator class about how much bad data was
   * removed. If, for example, the input had 100 rows and the output had ninety the report would
   * read:
   * Input data size:  100 rows
   * Output data size: 90 rows
   * Rows removed due to bad data: 10 rows (10%)
   */
  def generateBadDataReport(inputSize: Long, outputSize: Long) = List(reportInputSize(inputSize),
    reportOutputSize(outputSize), reportBadDataSize(inputSize, outputSize))


  /**
   * Returns a string describing the name of the results and where they are stored.
   * Has html <br> tags afterwards.
   */
  def reportOutputLocation(outputPath: String, resultName: String =
  "The results"): String = resultName + " are stored at: <br>" + outputPath + "<br>"

  /**
   * Add to a list that will be used by Tabulator. It creates an empty row in the table
   */
  val emptyRow = List("&nbsp", "&nbsp")
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



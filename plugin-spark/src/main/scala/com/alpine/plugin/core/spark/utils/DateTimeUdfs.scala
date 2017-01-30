package com.alpine.plugin.core.spark.utils

import java.sql.Timestamp

import org.apache.spark.sql.UserDefinedFunction
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

import scala.util.Try


object DateTimeUdfs extends Serializable {

  /**
    * This udf is used at write. It converts the java.sql.Timestamp object to a string in the correct
    * format specified (in Joda standard) by the column definition.
    * This is now "safe" if the date format cannot be parsed, this will return a null.
    */
  def toDateStringViaJoda(format: String): UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((timeStamp: java.sql.Timestamp) => {
      try {
        val millis = timeStamp.getTime
        val joda = new DateTime(millis)
        joda.toString(DateTimeFormat.forPattern(format))
      }
      catch {
        case e: Throwable => {
          if (e.isInstanceOf[NullPointerException] ||
            e.isInstanceOf[NumberFormatException] ||
            e.isInstanceOf[IllegalArgumentException]) {
            println("Exception converting  date time object to date format string. Could not parse string "
              + Try(timeStamp.toString).getOrElse(" null") + " to Joda date format " + format)
          } else {
            println(e.getMessage)
          }
          null
        }
      }
    })
  }

  /**
    * This udf is used at read. It is how we parse a date format from a String to the java TimeStamp
    * type that spark sql expects. We go via UTC time.
    * This is now "safe" if the date format cannot be parsed, this will return a null.
    */
  def nullableStringToTimeStampViaJoda(format: String): UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((dateString: String) => {
      try {
        val millis = DateTimeFormat.forPattern(format).parseDateTime(dateString).getMillis
        new Timestamp(millis)
      } catch {
        case (e: Throwable) => {
          if (e.isInstanceOf[NullPointerException] ||
            e.isInstanceOf[NumberFormatException] ||
            e.isInstanceOf[IllegalArgumentException]) {
            println("Exception converting date format string to date time object. Could not parse string "
              + dateString + " to Joda date format " + format)
          } else {
            println(e.getMessage)
          }
          null
        }
      }
    })
  }
}

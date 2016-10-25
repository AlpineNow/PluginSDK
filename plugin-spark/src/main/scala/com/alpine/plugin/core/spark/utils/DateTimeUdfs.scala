package com.alpine.plugin.core.spark.utils

import java.sql.Timestamp

import org.apache.spark.sql.UserDefinedFunction
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime


object DateTimeUdfs extends Serializable {

  def toUnixTimeStampViaJoda(format: String): UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((dateString: String) =>
      DateTimeFormat.forPattern(format).parseDateTime(dateString).getMillis)
  }

  def toTimestampFromUTC(format: String): UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((time: Long) => new Timestamp(time))
  }

  def toDateStringViaJoda(format: String): UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((timeStamp: java.sql.Timestamp) => {
      try {
        val millis = timeStamp.getTime
        val joda = new DateTime(millis)
        joda.toString(DateTimeFormat.forPattern(format))
      }
      catch {
        case e: Throwable => println("could not convert " + timeStamp.toString + " to format " + format); null
      }
    })
  }
}

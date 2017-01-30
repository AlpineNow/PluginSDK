package com.alpine.plugin.core.utils

import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite


class TimerTest extends FunSuite {

  test("Test Convert to Other Units Method ") {
    //testing for 2 hours, 10 minuts, 3 seconds, and 11 milliseconds
    val timeArray = Array(TimeUnit.HOURS,
      TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
    val times = Array[Long](2, 10, 3, 11)
    compareTimes(timeArray, times)

    compareTimes(Array(TimeUnit.SECONDS, TimeUnit.MILLISECONDS),
      Array[Long](3, 11))

    compareTimes(Array(TimeUnit.SECONDS), Array[Long](10L))

    compareTimes(Array(TimeUnit.HOURS,
      TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS), Array[Long](0, 0, 10, 20))
  }

  def compareTimes(timeUnits: Array[TimeUnit], times: Array[Long]): Unit = {
    val timerObject = new Timer()
    val totalMillies = timeUnits.zip(times).foldLeft(0L) {
      case (acc, (timeUnit, t)) => acc + timeUnit.toMillis(t)
    }
    val timeInHours = timerObject.convertMilliesToOtherUnits(totalMillies, timeUnits)
    timeInHours.zip(times).foreach {
      case (actual, expected) =>
        assert(actual === expected, "Arrays are not equal. Actual: " + actual + " Expected: " + expected)
    }
  }
}

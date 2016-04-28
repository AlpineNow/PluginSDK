package com.alpine.common.serialization.json

import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import org.scalatest.FunSuite

/**
  * Created by Rachel Warren on 4/27/16.
  */
class ColumnDefTypeAdapterTest extends FunSuite {
  val columnDef = ColumnDef("TestColumn", ColumnType.Double)
  val dateColumnDef = ColumnDef("DateColumn", ColumnType.DateTime("MM/dd/yy"))

  test("Should serialize a column with no format correctly") {

    JsonTestUtil.testJsonization(columnDef)
  }

  test("Test deserialization") {
    val testStringWithNoFormat = {
      """
        |{
        |  "columnName": "TestColumn",
        |  "columnType": "Double"
        |}
      """.stripMargin
    }

    JsonTestUtil.testDeserialization(testStringWithNoFormat, columnDef)
  }

  test("Should serialize a column with a format string") {
    JsonTestUtil.testJsonization(dateColumnDef)
  }

  test("Should deserialize correctly") {
    val testStringWithFormat =
      """
        |{
        |  "columnName": "DateColumn",
        |  "columnType": "DateTime",
        |  "columnTypeFormat": "MM/dd/yy"
        |}
      """.stripMargin

    JsonTestUtil.testDeserialization(testStringWithFormat, dateColumnDef)
  }

}

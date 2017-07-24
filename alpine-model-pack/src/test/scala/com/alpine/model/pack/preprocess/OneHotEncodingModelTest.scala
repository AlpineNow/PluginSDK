/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import com.alpine.json.{JsonTestUtil, ModelJsonUtil}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

/**
  * Tests serialization of OneHotEncodingModel
  * and application of OneHotEncodingTransformer.
  */
class OneHotEncodingModelTest extends FunSuite {

  import OneHotEncodingModelTest._

  /**
    * In February 2016 I changed the baseValue from String to Option[String].
    */
  private val modelFrom2015 =
    """{
      |  "oneHotEncodedFeatures": [
      |    {
      |      "hotValues": [
      |        "sunny",
      |        "overcast"
      |      ],
      |      "baseValue": "rain"
      |    },
      |    {
      |      "hotValues": [
      |        "true"
      |      ],
      |      "baseValue": "false"
      |    }
      |  ],
      |  "inputFeatures": [
      |    {
      |      "columnName": "outlook",
      |      "columnType": "String"
      |    },
      |    {
      |      "columnName": "wind",
      |      "columnType": "String"
      |    }
      |  ],
      |  "identifier": ""
      |}
      |""".stripMargin


  val oneHotEncoderModel = OneHotEncodingModel(Seq(
    OneHotEncodedFeature(List("sunny", "overcast"), "rain"),
    OneHotEncodedFeature(List("true"), "false")
  ),
    Seq(ColumnDef("outlook", ColumnType.String), ColumnDef("wind", ColumnType.String))
  )

  test("Should deserialize model from  2015") {
    val oldModel = ModelJsonUtil.compactGson.fromJson(modelFrom2015, classOf[OneHotEncodingModel])
    assert(oneHotEncoderModel === oldModel)
  }

  test("Serialization of the Pivot transformations should work") {
    val p = OneHotEncodedFeature(List("sunny", "overcast"), "rain")
    JsonTestUtil.testJsonization(p)
  }

  test("Serialization of the OneHotEncoder should work") {
    JsonTestUtil.testJsonization(oneHotEncoderModel)
  }

  test("Should transform input correctly") {
    val t = oneHotEncoderModel.transformer
    assert(Seq[Any](1, 0, 1) == t.apply(Seq[Any]("sunny", "true")))
    assert(Seq[Any](0, 1, 1) == t.apply(Seq[Any]("overcast", "true")))
    assert(Seq[Any](0, 0, 0) == t.apply(Seq[Any]("rain", "false")))
    intercept[Exception] {
      t.apply(Seq[Any]("stormy,true"))
    }
  }

  test("Should generate the correct SQL") {
    val model = (new OneHotEncodingModelTest).oneHotEncoderModel
    val transformer = OneHotEncodingSQLTransformer(model, new SimpleSQLGenerator)
    val sql = transformer.getSQL
    val expected: LayeredSQLExpressions = expectedSQLExpressions
    assert(expected === sql)
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[OneHotEncodingModel]).getSerialVersionUID === -7558600518234424483L)
  }

}

// This is used for several tests, so make it accessible.
object OneHotEncodingModelTest {
  def expectedSQLExpressions: LayeredSQLExpressions = {
    val expected = LayeredSQLExpressions(
      Seq(
        Seq(
          (ColumnarSQLExpression("""(CASE WHEN "outlook" IN ('sunny', 'overcast', 'rain') THEN "outlook" ELSE NULL END)"""), ColumnName("outlook")),
          (ColumnarSQLExpression("""(CASE WHEN "wind" IN ('true', 'false') THEN "wind" ELSE NULL END)"""), ColumnName("wind"))
        ),
        Seq(
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END)"""), ColumnName("outlook_0")),
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN "outlook" IS NOT NULL THEN 0 ELSE NULL END)"""), ColumnName("outlook_1")),
          (ColumnarSQLExpression("""(CASE WHEN ("wind" = 'true') THEN 1 WHEN "wind" IS NOT NULL THEN 0 ELSE NULL END)"""), ColumnName("wind_0"))
        )
      )
    )
    expected
  }
}

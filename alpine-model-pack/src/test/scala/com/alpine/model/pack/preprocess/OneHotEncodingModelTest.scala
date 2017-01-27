/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.json.{ModelJsonUtil, JsonTestUtil}
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

/**
 * Tests serialization of OneHotEncodingModel
 * and application of OneHotEncodingTransformer.
 */
class OneHotEncodingModelTest extends FunSuite {

  /**
    * In February 2016 I changed the baseValue from String to Option[String].
    */
  val modelFrom2015 = """{
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
    Seq(new ColumnDef("outlook", ColumnType.String), new ColumnDef("wind", ColumnType.String))
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
    assert(Seq[Any](1,0,1) == t.apply(Seq[Any]("sunny","true")))
    assert(Seq[Any](0,1,1) == t.apply(Seq[Any]("overcast","true")))
    assert(Seq[Any](0,0,0) == t.apply(Seq[Any]("rain","false")))
    intercept[Exception] {
      t.apply(Seq[Any]("stormy,true"))
    }
  }

  test("Should generate the correct SQL") {
    val model = (new OneHotEncodingModelTest).oneHotEncoderModel
    val transformer = OneHotEncodingSQLTransformer(model, new SimpleSQLGenerator)
    val sql = transformer.getSQL
    val expected = LayeredSQLExpressions(
      Seq(
        Seq(
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'sunny') THEN 1 WHEN ("outlook" = 'overcast') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_0")),
          (ColumnarSQLExpression("""(CASE WHEN ("outlook" = 'overcast') THEN 1 WHEN ("outlook" = 'sunny') OR ("outlook" = 'rain') THEN 0 ELSE NULL END)"""), ColumnName("outlook_1")),
          (ColumnarSQLExpression("""(CASE WHEN ("wind" = 'true') THEN 1 WHEN ("wind" = 'false') THEN 0 ELSE NULL END)"""), ColumnName("wind_0"))
        )
      )
    )

    assert(expected === sql)
  }

}

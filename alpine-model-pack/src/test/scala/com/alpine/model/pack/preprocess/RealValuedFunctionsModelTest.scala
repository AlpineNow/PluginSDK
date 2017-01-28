/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.json.{JsonTestUtil, ModelJsonUtil}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.sql.ColumnarSQLExpression
import com.alpine.util.{FilteredSeq, SimpleSQLGenerator}
import org.scalatest.FunSuite
/**
 * Tests serialization of RealValuedFunctionsModel
 * and application of RealValuedFunctionTransformer.
 */
class RealValuedFunctionsModelTest extends FunSuite {

  val inputFeatures = Seq(ColumnDef("x1", ColumnType.Double), ColumnDef("x2", ColumnType.Double))
  val functions = Seq((Exp(), 0), (Exp(), 1), (Log(), 1), (Power(2), 0), (LinearFunction(4, 5), 0))
  val model = RealValuedFunctionsModel(functions.map(t => RealFunctionWithIndex(TypeWrapper(t._1), t._2)), inputFeatures, "RV")

  test("Should serialize properly") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should score properly") {
    val scorer = model.transformer
    assert(Seq(math.exp(2), math.E, 0, 4, 13) === scorer(Seq(2,1.0)))
  }

  test("Should generate correct SQL") {
    val sql = model.sqlTransformer(new SimpleSQLGenerator).get.getSQLExpressions
    val expectedSQL = List(
      ColumnarSQLExpression("""EXP("x1")"""),
      ColumnarSQLExpression("""EXP("x2")"""),
      ColumnarSQLExpression("""LN("x2")"""),
      ColumnarSQLExpression("""POWER("x1", 2.0)"""),
      ColumnarSQLExpression("""4.0 * "x1" + 5.0""")
    )
    assert(expectedSQL === sql)
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[RealValuedFunctionsModel]).getSerialVersionUID === -6730039384877850890L)
  }

  test("Should streamline correctly") {
    val m1 = model.streamline(Seq("Exp_x1", "LinearFunction_x1"))
    val m2 = model.streamline(Seq("Exp_x2"))
    val mBoth = model.streamline(Seq("Log_x2", "Power_x1"))
    assert(m1.identifier === model.identifier)
    assert(m1.inputFeatures.map(_.columnName).toSet === Set("x1"))
    assert(m2.inputFeatures.map(_.columnName).toSet === Set("x2"))
    assert(mBoth.inputFeatures.map(_.columnName).toSet === Set("x1", "x2"))
    val (t, t1, t2, tBoth) = (model.transformer, m1.transformer, m2.transformer, mBoth.transformer)
    Range(0,5).foreach { _ =>
      val x1 = math.random
      val x2 = math.random
      val originalOutput = t.apply(Seq(x1, x2))
      val t1Output = t1.apply(Seq(x1))
      val t2Output = t2.apply(Seq(x2))
      val tBothOutput = tBoth.apply(Seq(x1, x2))
      assert(FilteredSeq(originalOutput, Seq(0, 4)) === t1Output)
      assert(FilteredSeq(originalOutput, Seq(1)) === t2Output)
      assert(FilteredSeq(originalOutput, Seq(2, 3)) === tBothOutput)
    }
  }

  test("Deserialization of old models should work") {
    val deserializedModel = ModelJsonUtil.compactGson.fromJson(oldModelJson, classOf[RealValuedFunctionsModel])
    assert(deserializedModel === model)
  }

  private val oldModelJson = """{
                       |  "functions": [
                       |    {
                       |      "function": {
                       |        "type": "com.alpine.model.pack.preprocess.Exp",
                       |        "data": {}
                       |      },
                       |      "index": 0
                       |    },
                       |    {
                       |      "function": {
                       |        "type": "com.alpine.model.pack.preprocess.Exp",
                       |        "data": {}
                       |      },
                       |      "index": 1
                       |    },
                       |    {
                       |      "function": {
                       |        "type": "com.alpine.model.pack.preprocess.Log",
                       |        "data": {}
                       |      },
                       |      "index": 1
                       |    },
                       |    {
                       |      "function": {
                       |        "type": "com.alpine.model.pack.preprocess.Power",
                       |        "data": {
                       |          "p": 2.0
                       |        }
                       |      },
                       |      "index": 0
                       |    },
                       |    {
                       |      "function": {
                       |        "type": "com.alpine.model.pack.preprocess.LinearFunction",
                       |        "data": {
                       |          "m": 4.0,
                       |          "c": 5.0
                       |        }
                       |      },
                       |      "index": 0
                       |    }
                       |  ],
                       |  "inputFeatures": [
                       |    {
                       |      "columnName": "x1",
                       |      "columnType": "Double"
                       |    },
                       |    {
                       |      "columnName": "x2",
                       |      "columnType": "Double"
                       |    }
                       |  ],
                       |  "identifier": "RV"
                       |}""".stripMargin

}

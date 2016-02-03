/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.transformer.sql.ColumnarSQLExpression
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite
/**
 * Tests serialization of RealValuedFunctionsModel
 * and application of RealValuedFunctionTransformer.
 */
class RealValuedFunctionsModelTest extends FunSuite {

  val inputFeatures = Seq(ColumnDef("x1", ColumnType.Double), ColumnDef("x2", ColumnType.Double))
  val functions = Seq((Exp(), 0), (Exp(), 1), (Log(), 1), (Power(2), 0))
  val model = RealValuedFunctionsModel(functions.map(t => RealFunctionWithIndex(TypeWrapper(t._1), t._2)), inputFeatures)

  test("Should serialize properly") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should score properly") {
    val scorer = model.transformer
    assert(Seq(math.exp(2), math.E, 0, 4) === scorer(Seq(2,1.0)))
  }

  test("Should generate correct SQL") {
    val sql = model.sqlTransformer(new SimpleSQLGenerator).get.getSQLExpressions
    val expectedSQL = List(
      ColumnarSQLExpression("""EXP("x1")"""),
      ColumnarSQLExpression("""EXP("x2")"""),
      ColumnarSQLExpression("""LN("x2")"""),
      ColumnarSQLExpression("""POWER("x1", 2.0)""")
    )
    assert(expectedSQL === sql)
  }

}

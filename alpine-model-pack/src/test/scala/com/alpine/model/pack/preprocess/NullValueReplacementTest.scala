/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import com.alpine.transformer.sql.ColumnarSQLExpression
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

/**
 * Tests serialization of NullValueReplacement
 * and application of NullValueReplacer.
 */
class NullValueReplacementTest extends FunSuite {

  val model = NullValueReplacement(
    Seq[Any](70, "sunny"),
    Seq(
      ColumnDef("humidity", ColumnType.Int),
      ColumnDef("outlook", ColumnType.String)
    )
  )

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should apply transformation correctly") {
    val t = model.transformer
    assert(Seq(70,"sunny") === t.apply(Seq[Any](null, null)))
    assert(Seq(65,"sunny") === t.apply(Seq[Any](65, null)))
    assert(Seq(70,"rainy") === t.apply(Seq[Any](null, "rainy")))
    assert(Seq(65,"rainy") === t.apply(Seq[Any](65, "rainy")))
  }

  test("Should generate SQL expression correctly") {
    val t = model.sqlTransformer(new SimpleSQLGenerator).get
    val sqlExpressions = t.getSQLExpressions
    val expected = List(
      ColumnarSQLExpression("""COALESCE("humidity", 70)"""),
      ColumnarSQLExpression("""COALESCE("outlook", 'sunny')""")
    )
    assert(expected === sqlExpressions)
  }
}

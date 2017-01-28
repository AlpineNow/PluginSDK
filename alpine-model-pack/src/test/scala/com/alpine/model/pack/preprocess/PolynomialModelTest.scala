/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import com.alpine.json.{JsonTestUtil, ModelJsonUtil}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.{FilteredSeq, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
 * Tests serialization of PolynomialModel
 * and application of PolynomialTransformer.
 */
class PolynomialModelTest extends FunSuite {

  val exponents = Seq(Seq[java.lang.Double](1.0,2.0,0.0), Seq[java.lang.Double](0.5,3.0,2.0))
  val inputFeatures: Seq[ColumnDef] = {
    Seq(new ColumnDef("x1", ColumnType.Double), new ColumnDef("x2", ColumnType.Double), new ColumnDef("x3", ColumnType.Double))
  }

  val t: PolynomialModel = new PolynomialModel(exponents, inputFeatures, "P")

  private val oldModelJson =
    """{"exponents":[[1.0,2.0,0.0],[0.5,3.0,2.0]],
      |"inputFeatures":[{"columnName":"x1","columnType":"Double"},{"columnName":"x2","columnType":"Double"},{"columnName":"x3","columnType":"Double"}],
      |"identifier":"P"}""".stripMargin

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(t)
  }

  test("Deserialization of old models should work") {
    val deserializedModel = ModelJsonUtil.compactGson.fromJson(oldModelJson, classOf[PolynomialModel])
    assert(deserializedModel === t)
  }

  test("Should score correctly") {
    assert(Seq(1d, 1d) === t.transformer.apply(Seq(1,1,1)))
    assert(Seq(1d, 0d) === t.transformer.apply(Seq(1,1,0)))
    assert(Seq(1 * 4d, 1 * 8 * 9d) === t.transformer.apply(Seq(1,2,3)))
    assert(Seq(4 * 1d, 2 * 1 * 2.25) === t.transformer.apply(Seq(4,1,1.5)))
  }

  test("Should generate correct SQL") {
    val sqlTransformer = t.sqlTransformer(new SimpleSQLGenerator).get
    val sql = sqlTransformer.getSQL
    val expected = LayeredSQLExpressions(Seq(Seq(
      (ColumnarSQLExpression("\"x1\" * POWER(\"x2\", 2.0)"), ColumnName("y_0")),
      (ColumnarSQLExpression("POWER(\"x1\", 0.5) * POWER(\"x2\", 3.0) * POWER(\"x3\", 2.0)"), ColumnName("y_1"))
    )))
    assert(expected === sql)
  }

  test("Should return 1 if all exponents are 0") {
    val emptyExponents = Seq(Seq[java.lang.Double](0.0,0.0,0.0), Seq[java.lang.Double](0.0,3.0,2.0))
    val model: PolynomialModel = new PolynomialModel(emptyExponents, inputFeatures)

    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val sql = sqlTransformer.getSQL
    val expected = LayeredSQLExpressions(Seq(Seq(
      (ColumnarSQLExpression("1"), ColumnName("y_0")),
      (ColumnarSQLExpression("POWER(\"x2\", 3.0) * POWER(\"x3\", 2.0)"), ColumnName("y_1"))
    )))
    assert(expected === sql)

    assert(Seq(1d, 1d) === model.transformer.apply(Seq(1,1,1)))
    assert(Seq(1d, 0d) === model.transformer.apply(Seq(1,1,0)))
    assert(Seq(1, 8 * 9d) === model.transformer.apply(Seq(1,2,3)))
    assert(Seq(1, 1 * 2.25) === model.transformer.apply(Seq(4,1,1.5)))
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[PolynomialModel]).getSerialVersionUID === -6759725743006372988L)
  }

  test("Should streamline correctly") {
    val model: PolynomialModel = PolynomialModel(
      Seq(Seq[java.lang.Double](1.0, 2.0, 0.0), Seq[java.lang.Double](0.5, 0.0, 2.0)),
      Seq(ColumnDef("a", ColumnType.Double), ColumnDef("b", ColumnType.Double), ColumnDef("c", ColumnType.Double))
    )
    val streamlinedModel = model.streamline(Seq("y_1"))
    assert(streamlinedModel.outputFeatures.map(_.columnName) === Seq("y_1"))
    // Should drop column b, because after we drop the y_0 row, then the remaining matrix has 0s in that column.
    assert(streamlinedModel.inputFeatures.map(_.columnName) === Seq("a", "c"))
    Range(0, 5).foreach(_ => {
      val row = Seq(math.random, math.random, math.random)
      assert(model.transformer.apply(row)(1) === streamlinedModel.transformer.apply(FilteredSeq(row, Seq(0, 2)))(0))
    })
  }

}

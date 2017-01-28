package com.alpine.model.pack.preprocess

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 3/18/16.
  */
class MatrixModelTest extends FunSuite {

  val values = Seq(Seq[java.lang.Double](1.0, 2.0, 0.0), Seq[java.lang.Double](0.5, 3.0, 2.0))
  private val inputFeatures = {
    Seq(ColumnDef("x1", ColumnType.Double), ColumnDef("x2", ColumnType.Double), ColumnDef("x3", ColumnType.Double))
  }

  val t = new MatrixModel(values, inputFeatures)

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(t)
  }

  test("Should score correctly") {
    assert(Seq(1 + 2, 0.5 + 3 + 2) === t.transformer.apply(Seq(1, 1, 1)))
    assert(Seq(1 + 2, 0.5 + 3) === t.transformer.apply(Seq(1, 1, 0)))
    assert(Seq(1 + 4, 0.5 + 6 + 6) === t.transformer.apply(Seq(1, 2, 3)))
    assert(Seq(4 + 2, 2 + 3 + 3) === t.transformer.apply(Seq(4, 1, 1.5)))
  }

  test("Should generate correct SQL") {
    val sqlTransformer = t.sqlTransformer(new SimpleSQLGenerator).get
    val sql = sqlTransformer.getSQL
    val expected = LayeredSQLExpressions(Seq(Seq(
      (ColumnarSQLExpression("\"x1\" * 1.0 + \"x2\" * 2.0"), ColumnName("y_0")),
      (ColumnarSQLExpression("\"x1\" * 0.5 + \"x2\" * 3.0 + \"x3\" * 2.0"), ColumnName("y_1"))
    )))
    assert(expected === sql)
  }

  test("Should return 0 if all coefficients are 0") {
    val emptyCoefficients = Seq(Seq[java.lang.Double](0.0, 0.0, 0.0), Seq[java.lang.Double](0.0, 3.0, 2.0))
    val model = new MatrixModel(emptyCoefficients, inputFeatures)

    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val sql = sqlTransformer.getSQL
    val expected = LayeredSQLExpressions(Seq(Seq(
      (ColumnarSQLExpression("0"), ColumnName("y_0")),
      (ColumnarSQLExpression("\"x2\" * 3.0 + \"x3\" * 2.0"), ColumnName("y_1"))
    )))
    assert(expected === sql)

    assert(Seq(0, 3 + 2) === model.transformer.apply(Seq(1, 1, 1)))
    assert(Seq(0, 3) === model.transformer.apply(Seq(1, 1, 0)))
    assert(Seq(0, 6 + 6) === model.transformer.apply(Seq(1, 2, 3)))
    assert(Seq(0, 3 + 3) === model.transformer.apply(Seq(4, 1, 1.5)))
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[MatrixModel]).getSerialVersionUID === 2821986496045802220L)
  }

}

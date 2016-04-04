/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.model.RowModel
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.{ColumnarSQLExpression, SQLTransformer}

/**
  * Model that creates output features that are polynomial combinations of the input features.
  *
  *
  * We use java.lang.Double for the type of the numeric values, because the scala Double type information
  * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
  *
  * If the exponents are a matrix
  * {{{[[1,2,0], [0.5,3,2]]}}}
  * Then the transformation of a row (x1, x2, x3) will be
  * {{{(y1, y2) = (x1 * x2 pow 2, sqrt(x1) * x2 pow 3 * x3 pow 2).}}}
  */
case class PolynomialModel(exponents: Seq[Seq[java.lang.Double]], inputFeatures: Seq[ColumnDef], override val identifier: String = "") extends RowModel {
  override def transformer: Transformer = PolynomialTransformer(this)

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] = Some(new PolynomialSQLTransformer(this, sqlGenerator))

  def outputFeatures = {
    exponents.indices.map(i => ColumnDef("y_" + i, ColumnType.Double))
  }

}

case class PolynomialTransformer(model: PolynomialModel) extends Transformer {

  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)

  private val exponents: Array[Array[Double]] = model.exponents.map(v => TransformerUtil.javaDoubleSeqToArray(v)).toArray

  override def apply(row: Row): Row = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    val result = Array.ofDim[Double](exponents.length)
    var i = 0
    while (i < exponents.length) {
      var x = 1.0
      var j = 0
      while (j < rowAsDoubleArray.length) {
        // math.pow(0,0) = 1 in Scala, so we do not have to check the 0 case.
        x *= math.pow(rowAsDoubleArray(j), exponents(i)(j))
        j += 1
      }
      result(i) = x
      i += 1
    }
    result
  }
}

case class PolynomialSQLTransformer(model: PolynomialModel, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {
  override def getSQLExpressions: Seq[ColumnarSQLExpression] = {
    model.exponents.map(doubles => {
      val expression = (doubles zip inputColumnNames).flatMap {
        case (exponent, name) =>
          if (exponent == 1) {
            // Slightly more efficient.
            Some(name.escape(sqlGenerator))
          } else if (exponent == 0) {
            None // Do this to avoid "ERROR: zero raised to zero is undefined".
          } else {
            Some(s"POWER(${name.escape(sqlGenerator)}, $exponent)")
          }
      }.mkString(" * ")
      if (expression != "") {
        expression
      } else {
        // 1 is the identity for multiplication.
        "1"
      }
    }
    ).map(ColumnarSQLExpression)
  }

}

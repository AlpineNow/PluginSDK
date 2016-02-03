/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.RowModel
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.model.pack.util.CastedDoubleSeq
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression}

/**
 * Interface for real-valued functions
 * (functions that map the real (double) numbers to real (double) numbers)
 * to be used in RealValuedFunctionsModel
 */
trait RealValuedFunction {
  def apply(x: Double): Double
  def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression]
}

case class Exp() extends RealValuedFunction {
  def apply(x: Double) = math.exp(x)

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""EXP(${name.escape(sqlGenerator)})"""
      )
    )
  }

}

case class Log() extends RealValuedFunction {
  def apply(x: Double) = math.log(x)

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""LN(${name.escape(sqlGenerator)})"""
      )
    )
  }
}

case class Log1p() extends RealValuedFunction {
  def apply(x: Double) = math.log1p(x)

  // TODO: Investigate if some databases support this function natively.
  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""LN(1 + ${name.escape(sqlGenerator)})"""
      )
    )
  }
}

//case class Sqrt() extends RealValuedFunction {
//  def apply(x: Double) = math.sqrt(x)
//}
//
//case class Inverse() extends RealValuedFunction {
//  def apply(x: Double) = 1/x
//}
//
//case class Identity() extends RealValuedFunction {
//  def apply(x: Double) = x
//}
//
//case class Square() extends RealValuedFunction {
//  def apply(x: Double) = x*x
//}

case class Power(p: Double) extends RealValuedFunction {
  def apply(x: Double) = math.pow(x, p)

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""POWER(${name.escape(sqlGenerator)}, $p)"""
      )
    )
  }
}

case class RealValuedFunctionsModel(functions: Seq[RealFunctionWithIndex], inputFeatures: Seq[ColumnDef], override val identifier: String = "") extends RowModel {
  override def transformer = RealValuedFunctionTransformer(this)

  override def outputFeatures = {
    functions.map(f => f.function.value.getClass.getSimpleName + "_" + inputFeatures(f.index).columnName).map(name => ColumnDef(name, ColumnType.Double))
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[RealValuedFunctionsSQLTransformer] = {
    val canBeScoredInSQL = this.functions.map {
      // Some functions may only work for some database types, or not at all.
      wrappedFunction => wrappedFunction.function.value.sqlExpression(ColumnName("dummyName"), sqlGenerator)
    }.forall(_.isDefined)
    if (canBeScoredInSQL) {
      Some(RealValuedFunctionsSQLTransformer(this, sqlGenerator))
    } else {
      None
    }
  }
}

case class RealFunctionWithIndex(function: TypeWrapper[_ <: RealValuedFunction], index: Int)

case class RealValuedFunctionTransformer(model: RealValuedFunctionsModel) extends Transformer {

  val functions = model.functions.map(t => (t.function.value, t.index)).toArray

  override def apply(row: Row): Row = {
    val result = Array.ofDim[Any](functions.length)
    val doubleRow = CastedDoubleSeq(row)
    var i = 0
    while (i < functions.length) {
      val functionsTuple = functions(i)
      result(i) = functionsTuple._1(doubleRow(functionsTuple._2))
      i += 1
    }
    result
  }
}

case class RealValuedFunctionsSQLTransformer(model: RealValuedFunctionsModel, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {

  override def getSQLExpressions: Seq[ColumnarSQLExpression] = {
    model.functions.map {
      wrappedFunction => wrappedFunction.function.value.sqlExpression(inputColumnNames(wrappedFunction.index), sqlGenerator).get
    }
  }

}

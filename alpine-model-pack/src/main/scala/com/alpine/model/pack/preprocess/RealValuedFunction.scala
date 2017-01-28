/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.RowModel
import com.alpine.model.export.pfa.expressions.FunctionExecute
import com.alpine.model.export.pfa.modelconverters.RealValuedFunctionsPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.multiple.PipelineRowModel
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
  def pfaRepresentation(inputReference: Any): Any
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

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("m.exp", inputReference)
  }

}

case class Multiply(coefficient: Double) extends RealValuedFunction  {
  def apply(x: Double): Double = x * coefficient

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""$coefficient * ${name.escape(sqlGenerator)}"""
      )
    )
  }

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("*", inputReference, coefficient)
  }
}

case class Divide(denominator: Double) extends RealValuedFunction  {
  def apply(x: Double): Double = x / denominator

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""${name.escape(sqlGenerator)} / $denominator"""
      )
    )
  }

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("/", inputReference, denominator)
  }
}
case class Add(offset: Double) extends RealValuedFunction  {
  def apply(x: Double): Double = x + offset

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""${name.escape(sqlGenerator)} + $offset"""
      )
    )
  }

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("+", inputReference, offset)
  }
}

case class Subtract(offset: Double) extends RealValuedFunction  {
  def apply(x: Double): Double = x - offset

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""${name.escape(sqlGenerator)} - $offset"""
      )
    )
  }

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("-", inputReference, offset)
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

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("m.ln", inputReference)
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

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("m.ln1p", inputReference)
  }
}

/**
  * Represents the linear function y = m * x + c.
  * Can be constructed out of Add and Multiply functions, but included for convenience.
  *
  * Added in SDK 1.9 (Alpine-Chorus 6.3).
  *
  * @param m multiplier
  * @param c constant offset
  */
case class LinearFunction(m: Double, c: Double) extends RealValuedFunction {

  def apply(x: Double): Double = m * x + c

  override def sqlExpression(name: ColumnName, sqlGenerator: SQLGenerator): Option[ColumnarSQLExpression] = {
    Some(
      ColumnarSQLExpression(
        s"""$m * ${name.escape(sqlGenerator)} + $c"""
      )
    )
  }

  override def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("+",
      FunctionExecute("*", inputReference, m),
      c
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

  def pfaRepresentation(inputReference: Any): Any = {
    FunctionExecute("**", inputReference, p)
  }
}

@SerialVersionUID(-6730039384877850890L)
case class RealValuedFunctionsModel(functions: Seq[RealFunctionWithIndex], inputFeatures: Seq[ColumnDef], override val identifier: String = "") extends RowModel with PFAConvertible{
  override def transformer = RealValuedFunctionTransformer(this)

  @transient lazy val outputFeatures: Seq[ColumnDef] = {
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

  override def getPFAConverter: PFAConverter = new RealValuedFunctionsPFAConverter(this)

  override def streamline(requiredOutputFeatureNames: Seq[String]): RowModel = {
    val indices: Seq[Int] = requiredOutputFeatureNames.map(name => this.outputFeatures.indexWhere(c => c.columnName == name))
    val functionsToKeep = indices.map(i => this.functions(i))
    val indexOfNeededInputFeatures: Seq[Int] = functionsToKeep.map(f => f.index).distinct.sortBy(index => index)
    val oldIndexToNewIndexMap = indexOfNeededInputFeatures.zipWithIndex.toMap
    val newModel = RealValuedFunctionsModel(
      functionsToKeep.map(f => RealFunctionWithIndex(f.function, oldIndexToNewIndexMap(f.index))),
      indexOfNeededInputFeatures.map(i => this.inputFeatures(i))
    )
    val renamingModel = RenamingModel(newModel.outputFeatures, requiredOutputFeatureNames)
    PipelineRowModel(Seq(newModel, renamingModel), this.identifier)
  }
}

case class RealFunctionWithIndex(function: TypeWrapper[_ <: RealValuedFunction], index: Int)

object RealValuedFunctionsModel {
  def make(functions: Seq[RealValuedFunction], inputFeatures: Seq[ColumnDef]): RealValuedFunctionsModel = {
    RealValuedFunctionsModel(functions.zipWithIndex.map(t => RealFunctionWithIndex(TypeWrapper(t._1), t._2)), inputFeatures)
  }
}

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

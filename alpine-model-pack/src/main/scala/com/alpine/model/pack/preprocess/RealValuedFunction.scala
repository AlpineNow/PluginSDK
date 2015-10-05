/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.RowModel
import com.alpine.model.pack.util.CastedDoubleSeq
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.Transformer

/**
 * Interface for real-valued functions
 * (functions that map the real (double) numbers to real (double) numbers)
 * to be used in RealValuedFunctionsModel
 */
trait RealValuedFunction {
  def apply(x: Double): Double
}

case class Exp() extends RealValuedFunction {
  def apply(x: Double) = math.exp(x)
}

case class Log() extends RealValuedFunction {
  def apply(x: Double) = math.log(x)
}

case class Log1p() extends RealValuedFunction {
  def apply(x: Double) = math.log1p(x)
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
}

case class RealValuedFunctionsModel(functions: Seq[RealFunctionWithIndex], inputFeatures: Seq[ColumnDef]) extends RowModel {
  override def transformer = RealValuedFunctionTransformer(this)

  override def outputFeatures = {
    functions.map(f => f.function.value.getClass.getSimpleName + "_" + inputFeatures(f.index).columnName).map(name => ColumnDef(name, ColumnType.Double))
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

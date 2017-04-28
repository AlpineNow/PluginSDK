/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper

/**
  * Created by Jennifer Thompson on 2/23/17.
  */
case class AddFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() + y.doubleValue()
      case _ => null
    }
  }
}

case class SubtractFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() - y.doubleValue()
      case _ => null
    }
  }
}

case class MultiplyFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() * y.doubleValue()
      case _ => null
    }
  }
}

case class DivideFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Long, y: Long) => x / y
      case (x: Int, y: Int) => x / y
      case (x: Long, y: Int) => x / y
      case (x: Int, y: Long) => x / y
      case (x: Number, y: Number) => x.doubleValue() / y.doubleValue()
      case _ => null
    }
  }
}

case class ModulusFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() % y.doubleValue()
      case _ => null
    }
  }
}

case class PowerFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => math.pow(x.doubleValue(), y.doubleValue())
      case _ => null
    }
  }
}

abstract class SingleArgumentDoubleFunction extends SingleArgumentASSTExpression {
  def function: Double => Double

  override def execute(input: Map[String, Any]): Any = {
    val argVal = argument.execute(input)
    argVal match {
      case x: Number => function(x.doubleValue())
      case _ => null
    }
  }
}

case class ExpFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.exp
}

case class SqrtFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.sqrt
}

case class CbrtFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.cbrt
}

case class NegativeFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = x => (-1) * x
}

case class AbsoluteValueFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.abs
}

case class CeilingFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.ceil
}

case class FloorFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.floor
}

case class NaturalLogFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.log
}

case class Log10Function(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.log10
}

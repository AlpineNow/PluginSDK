/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) + ($rightAsSQL) ")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) - ($rightAsSQL)")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) * ($rightAsSQL)")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) / ($rightAsSQL)")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"MOD($leftAsSQL, $rightAsSQL)")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"POWER($leftAsSQL, $rightAsSQL)")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"EXP(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class SqrtFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.sqrt

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"SQRT(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class CbrtFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.cbrt

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CBRT(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class NegativeFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = x => (-1) * x

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"- (${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class AbsoluteValueFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.abs

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"ABS(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class CeilingFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.ceil

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    // Could alternatively use CEIL, but it is less widely supported.
    ColumnarSQLExpression(s"CEILING(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class FloorFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.floor

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"FLOOR(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class NaturalLogFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.log

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"LN(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class Log10Function(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.log10

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"LOG(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class CosineFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.cos

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"COS(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class SineFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.sin

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"SIN(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class TangentFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.tan

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"TAN(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class InverseSineFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.asin

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"ASIN(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class InverseCosineFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.acos

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"ACOS(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class InverseTangentFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentDoubleFunction {
  override def function: Double => Double = math.atan

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"ATAN(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}
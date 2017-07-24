package com.alpine.model.pack.ast.expressions
import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

/**
  * Created by meganchanglee on 6/26/17.
  */


abstract class CastExpression extends ASTExpression {
  override def inputNames: Set[String] = Set()
}

case class CastAsDoubleExpression(arg: TypeWrapper[ASTExpression]) extends CastExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = arg.execute(input)
    x match {
      case x: Number => x.doubleValue()
      case x: String => x.toDouble
      case null => null
      case _ => Double.NaN
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${arg.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS DOUBLE)")
  }
}

case class CastAsIntegerExpression(arg: TypeWrapper[ASTExpression]) extends CastExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = arg.execute(input)
    x match {
      case x: Number => x.intValue()
      case x: String => x.toInt
      case null => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${arg.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS INTEGER)")
  }
}

case class CastAsLongExpression(arg: TypeWrapper[ASTExpression]) extends CastExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = arg.execute(input)
    x match {
      case x: Number => x.longValue()
      case x: String => x.toLong
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${arg.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS LONG)")
  }
}

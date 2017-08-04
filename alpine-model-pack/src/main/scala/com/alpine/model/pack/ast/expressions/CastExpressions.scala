package com.alpine.model.pack.ast.expressions
import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

/**
  * TODO: Add DB Specific types to SQL Generator and use them here.
  *
  * Created by meganchanglee on 6/26/17.
  */
case class CastAsDoubleExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = argument.execute(input)
    x match {
      case x: Number => x.doubleValue()
      case x: String => x.toDouble
      case null => null
      case _ => Double.NaN
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${argument.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS DOUBLE)")
  }
}

case class CastAsIntegerExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = argument.execute(input)
    x match {
      case x: Number => x.intValue()
      case x: String => x.toInt
      case null => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${argument.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS INTEGER)")
  }
}

case class CastAsLongExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val x = argument.execute(input)
    x match {
      case x: Number => x.longValue()
      case x: String => x.toLong
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"CAST(${argument.toColumnarSQL(sqlGenerator: SQLGenerator).sql} AS LONG)")
  }
}

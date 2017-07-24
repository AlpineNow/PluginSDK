/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression
import com.alpine.util.SQLUtility


/**
  * Created by Jennifer Thompson on 3/1/17.
  */
abstract class LiteralExpression extends ASTExpression {
  override def inputNames: Set[String] = Set()
}

case class LongLiteralExpression(l: Long) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Long = l

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(l.toString)
  }
}

case class StringLiteralExpression(s: String) extends LiteralExpression {
  override def execute(input: Map[String, Any]): String = s

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(SQLUtility.wrapInSingleQuotes(s))
  }
}

case class DoubleLiteralExpression(d: Double) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Double = d


  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(d.toString)
  }
}

case class BooleanLiteralExpression(boolean: Boolean) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Boolean = boolean

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(boolean.toString)
  }
}

case class NullLiteralExpression() extends LiteralExpression {
  override def execute(input: Map[String, Any]): Any = null

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression("NULL")
  }
}

/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions


/**
  * Created by Jennifer Thompson on 3/1/17.
  */
abstract class LiteralExpression extends ASTExpression {
  override def inputNames: Set[String] = Set()
}

case class LongLiteralExpression(l: Long) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Long = l
}

case class StringLiteralExpression(s: String) extends LiteralExpression {
  override def execute(input: Map[String, Any]): String = s
}

case class DoubleLiteralExpression(d: Double) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Double = d
}

case class BooleanLiteralExpression(boolean: Boolean) extends LiteralExpression {
  override def execute(input: Map[String, Any]): Boolean = boolean
}

case class NullLiteralExpression() extends LiteralExpression {
  override def execute(input: Map[String, Any]): Any = null
}
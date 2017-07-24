/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

/**
  * Created by Jennifer Thompson on 3/1/17.
  */

trait ASTExpression {
  def inputNames: Set[String]
  def execute(input: Map[String, Any]): Any

  def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression
}

object ASTExpression {
  implicit def extractValue(t: TypeWrapper[ASTExpression]): ASTExpression = t.value
  implicit def wrapValue(value: ASTExpression): TypeWrapper[ASTExpression] = TypeWrapper(value)
}

abstract class SingleArgumentASSTExpression extends ASTExpression {
  def argument: TypeWrapper[ASTExpression]
  def inputNames: Set[String] = argument.inputNames
}

abstract class BinaryASSTExpression extends ASTExpression {
  def left: TypeWrapper[ASTExpression]
  def right: TypeWrapper[ASTExpression]

  @transient lazy val inputNames: Set[String] = left.value.inputNames ++ right.value.inputNames
}

abstract class BinaryASSTExpressionWithNullHandling extends BinaryASSTExpression {
  final override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.value.execute(input)
    lazy val rightVal = right.value.execute(input)
    if (leftVal == null || rightVal == null) {
      null
    } else {
      executeForNonNullValues(leftVal, rightVal)
    }
  }

  def executeForNonNullValues(leftVal: Any, rightVal: Any): Any
}

case class NameReferenceExpression(name: String) extends ASTExpression {
  override def inputNames: Set[String] = Set(name)
  override def execute(input: Map[String, Any]): Any = input(name)

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val x = sqlGenerator.quoteIdentifier(name)
    ColumnarSQLExpression(x)
  }
}

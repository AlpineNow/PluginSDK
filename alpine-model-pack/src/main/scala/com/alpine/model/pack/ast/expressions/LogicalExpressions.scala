/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper

/**
  * Created by Jennifer Thompson on 2/23/17.
  */
case class AndExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression]) extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.execute(input)
    val rightVal = right.execute(input)
    (leftVal, rightVal) match {
      case (true, true) => true
      case (false, _) => false
      case (_, false) => false
      case _ => null
    }
  }
}

case class OrExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression]) extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.execute(input)
    val rightVal = right.execute(input)
    (leftVal, rightVal) match {
      case (false, false) => false
      case (true, _) => true
      case (_, true) => true
      case _ => null
    }
  }
}

case class CaseWhenExpression(conditions: Seq[WhenThenClause], elseExpr: TypeWrapper[ASTExpression]) extends ASTExpression {
  @transient lazy val inputNames: Set[String] = (conditions.flatMap(_.inputNames) ++ elseExpr.inputNames).toSet

  override def execute(input: Map[String, Any]): Any = {
    val i = conditions.iterator
    while (i.hasNext) {
      val next = i.next()
      if (next.whenExpr.execute(input).asInstanceOf[Boolean]) {
        return next.thenExpr.execute(input)
      }
    }
    elseExpr.execute(input)
  }
}

case class WhenThenClause(whenExpr: TypeWrapper[ASTExpression], thenExpr: TypeWrapper[ASTExpression]) {
  @transient lazy val inputNames: Set[String] = whenExpr.inputNames ++ thenExpr.inputNames
}

case class NotExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val arg = argument.execute(input)
    arg match {
      case true => false
      case false => true
      case _ => null
    }
  }
}

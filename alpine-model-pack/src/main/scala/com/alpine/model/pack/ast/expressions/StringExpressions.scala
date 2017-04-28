/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper

/**
  * Created by Jennifer Thompson on 2/23/17.
  */
case class ToUpperCaseFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val argVal = argument.execute(input)
    if (argVal == null) {
      null
    } else {
      argVal.toString.toUpperCase
    }
  }
}

case class ToLowerCaseFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val argVal = argument.execute(input)
    if (argVal == null) {
      null
    } else {
      argVal.toString.toLowerCase
    }
  }
}

case class StringLengthFunction(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val argVal = argument.execute(input)
    if (argVal == null) {
      null
    } else {
      argVal.toString.length
    }
  }
}

case class StringConcatenationFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpressionWithNullHandling {
  override def executeForNonNullValues(leftVal: Any, rightVal: Any): Any = {
    leftVal.toString + rightVal.toString
  }
}

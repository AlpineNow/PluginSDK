/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper

/**
  * https://www.postgresql.org/docs/8.4/static/functions-conditional.html
  * Created by Jennifer Thompson on 2/23/17.
  */
case class CoalesceFunction(arguments: Seq[TypeWrapper[ASTExpression]]) extends ASTExpression {
  override def inputNames: Set[String] = arguments.flatMap(_.inputNames).toSet

  override def execute(input: Map[String, Any]): Any = {
    var result: Any = null
    val i = arguments.iterator
    while (i.hasNext && result == null) {
      result = i.next.execute(input)
    }
    result
  }
}

case class IsNotNullExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    argument.execute(input) != null
  }
}

case class IsNullExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    argument.execute(input) == null
  }
}

// https://msdn.microsoft.com/en-us/library/ms177562.aspx
case class NullIfFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression]) extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.execute(input)
    if (leftVal == right.execute(input)) {
      null
    } else {
      leftVal
    }
  }
}
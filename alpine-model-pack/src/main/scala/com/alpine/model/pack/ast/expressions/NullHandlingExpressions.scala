/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val inner = arguments.map { a => a.value.toColumnarSQL(sqlGenerator).sql }.mkString(", ")
    ColumnarSQLExpression(s"COALESCE($inner)")
  }
}

case class IsNullExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    argument.execute(input) == null
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"(${argument.toColumnarSQL(sqlGenerator).sql}) IS NULL")
  }
}

case class IsNotNullExpression(argument: TypeWrapper[ASTExpression]) extends SingleArgumentASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    argument.execute(input) != null
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"(${argument.toColumnarSQL(sqlGenerator).sql}) IS NOT NULL")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"NULLIF($leftAsSQL, $rightAsSQL)")
  }
}
/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.{DatabaseType, SQLGenerator}
import com.alpine.transformer.sql.ColumnarSQLExpression

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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"UPPER(${argument.toColumnarSQL(sqlGenerator).sql})")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    ColumnarSQLExpression(s"LOWER(${argument.toColumnarSQL(sqlGenerator).sql})")
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

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val lengthFunctionName = {
      if (sqlGenerator.dbType == DatabaseType.greenplum) {
        "LEN"
      } else {
        "LENGTH"
      }
    }
    ColumnarSQLExpression(s"$lengthFunctionName(${argument.toColumnarSQL(sqlGenerator).sql})")
  }
}

case class StringConcatenationFunction(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpressionWithNullHandling {
  override def executeForNonNullValues(leftVal: Any, rightVal: Any): Any = {
    leftVal.toString + rightVal.toString
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) || ($rightAsSQL)")
  }
}

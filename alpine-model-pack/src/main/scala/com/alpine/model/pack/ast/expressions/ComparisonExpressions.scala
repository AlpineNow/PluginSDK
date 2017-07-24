/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast.expressions

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.ColumnarSQLExpression

/**
  * Created by Jennifer Thompson on 2/23/17.
  */
case class GreaterThanExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() > y.doubleValue()
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) > ($rightAsSQL)")
  }
}

case class GreaterThanOrEqualsExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() >= y.doubleValue()
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) >= ($rightAsSQL)")
  }
}

case class LessThanExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() < y.doubleValue()
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) < ($rightAsSQL)")
  }
}

case class LessThanOrEqualsExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    (left.execute(input), right.execute(input)) match {
      case (x: Number, y: Number) => x.doubleValue() <= y.doubleValue()
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) <= ($rightAsSQL)")
  }
}

class BetweenExpression(val value: TypeWrapper[ASTExpression], val min: TypeWrapper[ASTExpression], val max: TypeWrapper[ASTExpression])
  extends ASTExpression {
  @transient lazy val inputNames: Set[String] = value.inputNames ++ min.inputNames ++ max.inputNames
  override def execute(input: Map[String, Any]): Any = {
    (value.execute(input), min.execute(input), max.execute(input)) match {
      case (v: Number, minimum: Number, maximum: Number) =>
        v.doubleValue() >= minimum.doubleValue() && maximum.doubleValue() >= v.doubleValue()
      case _ => null
    }
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val minAsSQL = min.toColumnarSQL(sqlGenerator).sql
    val maxAsSQL = max.toColumnarSQL(sqlGenerator).sql
    val valueAsSQL = value.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($valueAsSQL) BETWEEN ($minAsSQL) AND ($maxAsSQL)")
  }
}

case class EqualsExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpressionWithNullHandling {
  override def executeForNonNullValues(leftVal: Any, rightVal: Any): Any = {
    leftVal == rightVal
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) = ($rightAsSQL)")
  }
}

case class NotEqualsExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpressionWithNullHandling {
  override def executeForNonNullValues(leftVal: Any, rightVal: Any): Any = {
    leftVal != rightVal
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) <> ($rightAsSQL)")
  }
}

case class IsDistinctFromExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.execute(input)
    val rightVal = right.execute(input)
    leftVal != rightVal
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) IS DISTINCT FROM ($rightAsSQL)")
  }
}

case class IsNotDistinctFromExpression(left: TypeWrapper[ASTExpression], right: TypeWrapper[ASTExpression])
  extends BinaryASSTExpression {
  override def execute(input: Map[String, Any]): Any = {
    val leftVal = left.execute(input)
    val rightVal = right.execute(input)
    leftVal == rightVal
  }

  override def toColumnarSQL(sqlGenerator: SQLGenerator): ColumnarSQLExpression = {
    val leftAsSQL = left.toColumnarSQL(sqlGenerator).sql
    val rightAsSQL = right.toColumnarSQL(sqlGenerator).sql
    ColumnarSQLExpression(s"($leftAsSQL) IS NOT DISTINCT FROM ($rightAsSQL)")
  }
}

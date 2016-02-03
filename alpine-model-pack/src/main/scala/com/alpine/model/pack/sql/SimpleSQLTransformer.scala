/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.sql

import com.alpine.transformer.sql._

trait SimpleSQLTransformer extends SQLTransformer {

  def getSQL: LayeredSQLExpressions = {
    LayeredSQLExpressions(Seq(getSQLExpressions zip outputColumnNames))
  }

  def getSQLExpressions: Seq[ColumnarSQLExpression]
}
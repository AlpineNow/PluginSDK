/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.ml.sql

import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{ColumnarSQLExpression, RegressionModelSQLExpression, RegressionSQLTransformer}
import com.alpine.util.SQLUtility

class LinearRegressionSQLTransformer(val model: LinearRegressionModel, sqlGenerator: SQLGenerator) extends RegressionSQLTransformer {

  def predictionExpression: String = {
    model.intercept + " + " +
      SQLUtility.dotProduct(
        inputColumnNames.map(name => name.escape(sqlGenerator)),
        model.coefficients.map(_.toString)
      )
  }

  override def getPredictionSQL: RegressionModelSQLExpression = {
    RegressionModelSQLExpression(ColumnarSQLExpression(predictionExpression))
  }

}

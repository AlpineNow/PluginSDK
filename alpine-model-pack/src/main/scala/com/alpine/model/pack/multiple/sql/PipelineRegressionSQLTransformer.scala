/*
 * COPYRIGHT (C) Jan 28 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.{PipelineRowModel, PipelineRegressionModel}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{RegressionModelSQLExpression, RegressionSQLTransformer}

/**
  * Created by Jennifer Thompson on 1/28/16.
  */
class PipelineRegressionSQLTransformer(val model: PipelineRegressionModel, sqlGenerator: SQLGenerator)  extends RegressionSQLTransformer {

  override def getPredictionSQL: RegressionModelSQLExpression = {
    val sqlT = new PipelineRowModel(model.preProcessors).sqlTransformer(sqlGenerator).get.getSQL
    val lastSQL = model.finalModel.sqlTransformer(sqlGenerator).get.getPredictionSQL
    RegressionModelSQLExpression(lastSQL.predictionColumnSQL, sqlT.layers ++ lastSQL.intermediateLayers)
  }

}

object PipelineRegressionSQLTransformer {

  def make(model: PipelineRegressionModel, sqlGenerator: SQLGenerator): Option[PipelineRegressionSQLTransformer] = {
    val canBeScoredInSQL =
      model.preProcessors.map(_.sqlTransformer(sqlGenerator)).forall(_.isDefined) &&
      model.finalModel.sqlTransformer(sqlGenerator).isDefined
    if (canBeScoredInSQL) {
      Some(new PipelineRegressionSQLTransformer(model, sqlGenerator))
    } else {
      None
    }
  }

}

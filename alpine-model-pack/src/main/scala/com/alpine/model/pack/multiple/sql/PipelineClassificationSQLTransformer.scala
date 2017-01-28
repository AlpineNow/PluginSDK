/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.{PipelineRowModel, PipelineClassificationModel}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql._

class PipelineClassificationSQLTransformer(val model: PipelineClassificationModel, sqlGenerator: SQLGenerator) extends ClassificationSQLTransformer {

  override def getClassificationSQL: ClassificationModelSQLExpressions = {
    val sqlT = PipelineRowModel(model.preProcessors).sqlTransformer(sqlGenerator).get.getSQL
    val lastSQL = model.finalModel.sqlTransformer(sqlGenerator).get.getClassificationSQL
    ClassificationModelSQLExpressions(lastSQL.labelColumnSQL, lastSQL.confidenceSQL, sqlT.layers ++ lastSQL.intermediateLayers)
  }

}

object PipelineClassificationSQLTransformer {

  def make(model: PipelineClassificationModel, sqlGenerator: SQLGenerator): Option[PipelineClassificationSQLTransformer] = {
    val canBeScoredInSQL = model.preProcessors.map(_.sqlTransformer(sqlGenerator)).forall(_.isDefined) && model.finalModel.sqlTransformer(sqlGenerator).isDefined
    if (canBeScoredInSQL) {
      Some(new PipelineClassificationSQLTransformer(model, sqlGenerator))
    } else {
      None
    }
  }

}

/*
 * COPYRIGHT (C) Jan 28 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.{PipelineClusteringModel, PipelineRowModel}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{ClusteringModelSQLExpressions, ClusteringSQLTransformer}

/**
  * Created by Jennifer Thompson on 1/28/16.
  */
class PipelineClusteringSQLTransformer(val model: PipelineClusteringModel, sqlGenerator: SQLGenerator) extends ClusteringSQLTransformer {

  override def getClusteringSQL: ClusteringModelSQLExpressions = {
    val sqlT = PipelineRowModel(model.preProcessors).sqlTransformer(sqlGenerator).get.getSQL
    val lastSQL = model.finalModel.sqlTransformer(sqlGenerator).get.getClusteringSQL
    ClusteringModelSQLExpressions(lastSQL.labelColumnSQL, lastSQL.distanceSQL, sqlT.layers ++ lastSQL.intermediateLayers)
  }

}

object PipelineClusteringSQLTransformer {

  def make(model: PipelineClusteringModel, sqlGenerator: SQLGenerator): Option[PipelineClusteringSQLTransformer] = {
    val canBeScoredInSQL = model.preProcessors.map(_.sqlTransformer(sqlGenerator)).forall(_.isDefined) && model.finalModel.sqlTransformer(sqlGenerator).isDefined
    if (canBeScoredInSQL) {
      Some(new PipelineClusteringSQLTransformer(model, sqlGenerator))
    } else {
      None
    }
  }

}

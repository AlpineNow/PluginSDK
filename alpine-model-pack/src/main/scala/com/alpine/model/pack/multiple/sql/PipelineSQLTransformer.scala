/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.PipelineRowModel
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql._

case class PipelineSQLTransformer(model: PipelineRowModel, sqlGenerator: SQLGenerator) extends SQLTransformer {

  override def getSQL: LayeredSQLExpressions = {
    val transformers = model.transformers.map(_.sqlTransformer(sqlGenerator).get)
    val sqlT = transformers.flatMap(transformer => {
      transformer.getSQL.layers
    })
    LayeredSQLExpressions(sqlT)
  }

}

object PipelineSQLTransformer {

  def make(model: PipelineRowModel, sqlGenerator: SQLGenerator): Option[PipelineSQLTransformer] = {
    val sqlTransformers = model.transformers.map(_.sqlTransformer(sqlGenerator))
    val canBeScoredInSQL = sqlTransformers.forall(_.isDefined)
    if (canBeScoredInSQL) {
      Some(new PipelineSQLTransformer(model, sqlGenerator))
    } else {
      None
    }
  }

}

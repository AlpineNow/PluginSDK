/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.RowModel
import com.alpine.model.pack.ast.expressions.ASTExpression
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.transformer.Transformer

/**
  * Created by Jennifer Thompson on 2/22/17.
  */
// TODO: SqlTransformer and PFAConverter.
case class ASTModel(inputFeatures: Seq[ColumnDef],
                    outputFeatures: Seq[ColumnDef],
                    expressions: Seq[TypeWrapper[ASTExpression]])
  extends RowModel {
  override def transformer: Transformer = new Transformer {
    override def apply(row: Row): Seq[Any] = {
      val inputMap = (inputFeatures zip row).map {
        case (ColumnDef(columnName, _), value) => (columnName, value)
      }.toMap
      expressions.map(e => e.execute(inputMap))
    }
  }
}

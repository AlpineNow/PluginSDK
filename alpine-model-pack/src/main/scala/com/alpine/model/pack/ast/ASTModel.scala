/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ast

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.RowModel
import com.alpine.model.pack.ast.expressions.ASTExpression
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, LayeredSQLExpressions, SQLTransformer}

/**
  * Created by Jennifer Thompson on 2/22/17.
  */
// TODO: PFAConverter.
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

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] =
    Some(new ASTModelSQLTransformer(this, sqlGenerator))
}

class ASTModelSQLTransformer(val model: ASTModel, sqlGenerator: SQLGenerator) extends SQLTransformer {

  override def getSQL: LayeredSQLExpressions = {
    val expressions: Seq[(ColumnarSQLExpression, ColumnName)] = (model.expressions zip model.outputFeatures).map {
      case (TypeWrapper(e), columnDef) => (e.toColumnarSQL(sqlGenerator), ColumnName(columnDef.columnName))
    }
    LayeredSQLExpressions(Seq(expressions))
  }

}

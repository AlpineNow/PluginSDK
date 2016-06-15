/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.modelconverters.NullValueReplacementPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.ColumnarSQLExpression
import com.alpine.util.SQLUtility

/**
 * Model that will replace null values in the input row with specified values.
 */
case class NullValueReplacement(replacementValues: Seq[Any], inputFeatures: Seq[ColumnDef], override val identifier: String = "")
  extends RowModel with PFAConvertible  {

  override def transformer: Transformer = NullValueReplacer(this)
  override def outputFeatures = inputFeatures
  override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(new NullValueSQLReplacer(this, sqlGenerator))
  override def getPFAConverter: PFAConverter = new NullValueReplacementPFAConverter(this)
}

case class NullValueReplacer(model: NullValueReplacement) extends Transformer {

  val replacementValues = model.replacementValues.toArray

  override def allowNullValues = true

  override def apply(row: Row): Row = {
    val result = Array.ofDim[Any](row.length)
    var i = 0
    while (i < row.length) {
      val x = row(i)
      result(i) = if (x == null) {
        replacementValues(i)
      } else {
        x
      }
      i += 1
    }
    result
  }
}

/**
  * Assumes replacement values are either numeric or string.
  */
class NullValueSQLReplacer(val model: NullValueReplacement, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {

  override def getSQLExpressions: Seq[ColumnarSQLExpression] = {
    (inputColumnNames zip model.replacementValues).map {
      case (name, value) =>
        val replacementAsSQL = if (value.isInstanceOf[Number]) value else SQLUtility.wrapInSingleQuotes(value.toString)
        val escapedName = name.escape(sqlGenerator)
        s"""COALESCE($escapedName, $replacementAsSQL)"""
    }.map(ColumnarSQLExpression)
  }

}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.modelconverters.UnitPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.SQLTransformer

/**
 * Represents a model that carries features through without transforming them.
 * Designed to be used in parallel to other models in the CombinerModel.
 */
@SerialVersionUID(-75224626855321481L)
case class UnitModel(inputFeatures: Seq[ColumnDef], override val identifier: String = "")
  extends RowModel with PFAConvertible {
  override def transformer: Transformer = UnitTransformer
  override def outputFeatures = inputFeatures

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] = {
    Some(UnitSQLTransformer(this, sqlGenerator))
  }

  override def getPFAConverter: PFAConverter = new UnitPFAConverter(this)

  override def streamline(requiredOutputFeatureNames: Seq[String]): UnitModel = {
    UnitModel(inputFeatures.filter(c => requiredOutputFeatureNames.contains(c.columnName)), this.identifier)
  }
}

/**
 * Applies the unit (a.k.a. identity or no-operation) transformation to the input row.
 * That is, apply returns its input argument.
 */
object UnitTransformer extends Transformer {
  override def apply(row: Row): Row = row
}

case class UnitSQLTransformer(model : UnitModel, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {
  override def getSQLExpressions = {
    inputColumnNames.map(_.asColumnarSQLExpression(sqlGenerator))
  }
}
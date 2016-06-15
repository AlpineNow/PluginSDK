/*
 * COPYRIGHT (C) Feb 12 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple

import com.alpine.model.export.pfa.modelconverters.GroupByPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.multiple.sql.{GroupByClassificationSQLTransformer, GroupByRegressionSQLTransformer}
import com.alpine.model.{ClassificationRowModel, RegressionRowModel, RowModel}
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.RegressionTransformer
import com.alpine.transformer.sql.{ClassificationSQLTransformer, RegressionSQLTransformer}

/**
  * Created by Jennifer Thompson on 2/12/16.
  */
trait GroupByModel[M <: RowModel] {

  def groupByFeature: ColumnDef

  def modelsByGroup: Map[Any, M]

  @transient lazy val inputFeatures: Seq[ColumnDef] = groupByFeature :: modelsByGroup.flatMap(_._2.inputFeatures).toList.distinct

  protected def sqlScorable(sqlGenerator: SQLGenerator): Boolean = {
    modelsByGroup.values.map(model => model.sqlTransformer(sqlGenerator)).forall(_.isDefined)
  }
}

/**
  * Created by Jennifer Thompson on 2/9/16.
  * The keys in modelsByGroup need to be primitive (String, Long, Double etc) to work with serialization.
  * They also need to be of the type of the groupByFeature.
  */
case class GroupByRegressionModel(groupByFeature: ColumnDef, modelsByGroup: Map[Any, RegressionRowModel], override val identifier: String = "")
  extends RegressionRowModel with GroupByModel[RegressionRowModel] with PFAConvertible {

  def isValid: Boolean = {
    modelsByGroup.nonEmpty && modelsByGroup.map(_._2.dependentFeature).toSeq.distinct.size == 1
  }

  assert(isValid)

  override def transformer: RegressionTransformer = new GroupByRegressionTransformer(this)

  override def dependentFeature: ColumnDef = modelsByGroup.head._2.dependentFeature

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[RegressionSQLTransformer] = {
    if (sqlScorable(sqlGenerator)) {
      Some(new GroupByRegressionSQLTransformer(this, sqlGenerator))
    } else {
      None
    }
  }

  override def getPFAConverter: PFAConverter = new GroupByPFAConverter(this)
}

case class GroupByClassificationModel(groupByFeature: ColumnDef, modelsByGroup: Map[Any, ClassificationRowModel], override val identifier: String = "")
  extends ClassificationRowModel with GroupByModel[ClassificationRowModel] with PFAConvertible {

  def isValid: Boolean = {
    modelsByGroup.nonEmpty && modelsByGroup.map(_._2.dependentFeature).toSeq.distinct.size == 1
  }

  assert(isValid)

  override def transformer = new GroupByClassificationTransformer(this)

  @transient lazy val dependentFeature: ColumnDef = modelsByGroup.head._2.dependentFeature

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[ClassificationSQLTransformer] = {
    if (sqlScorable(sqlGenerator)) {
      Some(new GroupByClassificationSQLTransformer(this, sqlGenerator))
    } else {
      None
    }
  }

  @transient lazy val classLabels: Seq[String] = modelsByGroup.flatMap(_._2.classLabels).toList.distinct

  override def getPFAConverter: PFAConverter = new GroupByPFAConverter(this)

}

/*
 * COPYRIGHT (C) Feb 12 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.multiple.{CombinerModel, GroupByClassificationModel}
import com.alpine.model.{ClassificationRowModel, RowModel}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql._
import com.alpine.util.SQLUtility

/**
  * Created by Jennifer Thompson on 2/12/16.
  */
class GroupByClassificationSQLTransformer(val model: GroupByClassificationModel, val sqlGenerator: SQLGenerator) extends ClassificationSQLTransformer {

  override def getClassificationSQL = {
    val modelsByGroup = model.modelsByGroup.toSeq

    /**
      * TODO:
      * We could make this more efficient by consolidating duplicate expressions,
      * but this will work for now.
      */
    val combinedModels = CombinerModel.make(modelsByGroup.map(m => ConfidenceColumnsModel(m._2)) ++ Seq(new UnitModel(Seq(model.groupByFeature))))

    val combinedIntermediateLayers = combinedModels.sqlTransformer(sqlGenerator).get.getSQL

    val groupBySQLExpression = combinedIntermediateLayers.layers.last.last._2.asColumnarSQLExpression(sqlGenerator)

    val startingFeatureIndexByModel = modelsByGroup.map(_._1) zip cumulativeSum(modelsByGroup.map(_._2.classLabels.size))

    val finalLayerConfidences = model.classLabels.map(l => {
      (l, SQLUtility.groupBySQL(
        groupBySQLExpression,
        startingFeatureIndexByModel.map {
          case (value, startingIndex) =>
            val offset = model.modelsByGroup.get(value).get.classLabels.indexOf(l)
            val valueAsExpression: ColumnarSQLExpression = ColumnarSQLExpression(SQLUtility.wrapAsValue(value))
            if (offset > -1) {
              (valueAsExpression, combinedIntermediateLayers.layers.last(startingIndex + offset)._2.asColumnarSQLExpression(sqlGenerator))
            } else {
              (valueAsExpression, ColumnarSQLExpression("0"))
            }
        }.toMap
      ))
    })

    val confidenceColumnNames = model.classLabels.indices.map(i => ColumnName("CONF" + i))

    val intermediateLayers = combinedIntermediateLayers.layers ++ Seq(finalLayerConfidences.map(_._2) zip confidenceColumnNames)

    ClassificationModelSQLExpressions.apply(finalLayerConfidences.map(_._1) zip confidenceColumnNames, intermediateLayers, sqlGenerator)
  }

  private def cumulativeSum(values: Seq[Int]): Seq[Int] = {
    values.foldLeft(List[Int](0))((sumSoFar, value) => {
      (value + sumSoFar.head) :: sumSoFar
    }).reverse
  }

  /**
    * This will probably be made a fully fledged standalone class at some point,
    * but it's not clean enough yet (have to resolve what to do for the in-memory transformer).
    *
    * For now we keep it here to only be instantiated in the GroupByClassificationSQLTransformer.
    */
  private case class ConfidenceColumnsModel(c: ClassificationRowModel) extends RowModel {
    override def transformer: Transformer = ???

    override def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] = {
      Some(
        new SQLTransformer() {

          override def getSQL: LayeredSQLExpressions = {
            val transformer = c.sqlTransformer(sqlGenerator).get
            val classificationSQL = transformer.getClassificationSQL
            val finalLayerConfidences = c.classLabels.map(l => classificationSQL.confidenceSQL(l)).toList
            val finalLayer = Seq(finalLayerConfidences zip outputColumnNames)
            LayeredSQLExpressions(classificationSQL.intermediateLayers ++ finalLayer)
          }

          override def model: RowModel = ConfidenceColumnsModel.this
        }
      )
    }

    override def inputFeatures = c.inputFeatures

    override def sqlOutputFeatures: Seq[ColumnDef] = {
      c.classLabels.indices.map(i => ColumnDef("CONF" + i, ColumnType.Double))
    }

    override def outputFeatures: Seq[ColumnDef] = ???
  }

}

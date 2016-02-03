/*
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.transformer.sql

import com.alpine.model.{RowModel, RegressionRowModel, ClassificationRowModel, ClusteringRowModel}
import com.alpine.plugin.core.io.ColumnDef

trait SQLTransformer {

  def inputFeatures: Seq[ColumnDef] = model.inputFeatures

  def inputColumnNames = inputFeatures.map(f => ColumnName(f.columnName))

  def outputFeatures: Seq[ColumnDef] = model.sqlOutputFeatures

  def outputColumnNames = outputFeatures.map(f => ColumnName(f.columnName))

  def model: RowModel

  def identifier: String = ""

  def getSQL: LayeredSQLExpressions

}

trait ClassificationSQLTransformer extends SQLTransformer {

  def getClassificationSQL: ClassificationModelSQLExpressions

  def model: ClassificationRowModel

  override def getSQL: LayeredSQLExpressions = {
    val classificationSQL = getClassificationSQL
    val finalLayer = Seq((classificationSQL.labelColumnSQL :: Nil) zip outputColumnNames)
    LayeredSQLExpressions(classificationSQL.intermediateLayers ++ finalLayer)
  }

}

trait ClusteringSQLTransformer extends SQLTransformer {

  def getClusteringSQL: ClusteringModelSQLExpressions

  def model: ClusteringRowModel

  override def getSQL: LayeredSQLExpressions = {
    val clusteringSQL = getClusteringSQL
    val finalLayer = Seq((clusteringSQL.labelColumnSQL :: Nil) zip outputColumnNames)
    LayeredSQLExpressions(clusteringSQL.intermediateLayers ++ finalLayer)
  }

}

trait RegressionSQLTransformer extends SQLTransformer {

  def getPredictionSQL: RegressionModelSQLExpression

  def model: RegressionRowModel

  override def getSQL: LayeredSQLExpressions = {
    val predictionSQL = getPredictionSQL
    LayeredSQLExpressions(
      predictionSQL.intermediateLayers ++ Seq(Seq((predictionSQL.predictionColumnSQL, outputColumnNames.head)))
    )
  }

}


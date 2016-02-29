/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model

import com.alpine.metadata.DetailedTransformationSchema
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.result._
import com.alpine.sql.SQLGenerator
import com.alpine.transformer._
import com.alpine.transformer.sql.{ClassificationSQLTransformer, ClusteringSQLTransformer, RegressionSQLTransformer, SQLTransformer}
import com.alpine.util.FeatureUtil

/**
 * These objects are serialized to JSON via GSON, augmented with type adapters for known interfaces,
 * scala Seq collections and Options.
 */

trait MLModel extends Serializable {
  /**
   * This should return a representative class from every jar that is needed to load
   * the object during deserialization.
   *
   * Default implementation returns the class of MLModel,
   * and the class of the model implementation.
   */
  def classesForLoading: Set[Class[_]] = Set[Class[_]](classOf[MLModel], this.getClass)
}

/**
 * RowModels are models that will produce a single result per row of data.
 */
trait RowModel extends MLModel {
  def transformer: Transformer
  def inputFeatures: Seq[ColumnDef]
  def outputFeatures: Seq[ColumnDef]
  def sqlOutputFeatures: Seq[ColumnDef] = outputFeatures
  def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] = None
  /**
   * Used to identify this model when in a collection of models.
   * Should be simple characters, so it can be used in a feature name.
   *
   * @return identifier for the model.
   */
  def identifier: String = ""
  def transformationSchema: DetailedTransformationSchema = {
    DetailedTransformationSchema(inputFeatures, outputFeatures, identifier)
  }
}

/**
 * A model that predicts values in a predefined set of classes.
 */
trait CategoricalRowModel extends RowModel {
  /**
   * The total set of classes that the predicted value can take on.
    *
    * @return The set of possible values for the predicted value.
   */
  def classLabels: Seq[String]
  def transformer: CategoricalTransformer[_ <: CategoricalResult]

  override def sqlOutputFeatures: Seq[ColumnDef] = {
    Seq(ColumnDef(FeatureUtil.PRED, ColumnType.String))
  }
}

trait ClassificationRowModel extends CategoricalRowModel {
  def transformer: ClassificationTransformer
  def outputFeatures = FeatureUtil.classificationOutputFeatures
  /**
   * Used when we are doing model quality evaluation e.g. Confusion Matrix,
   * so we know what feature in the test dataset to compare the result to.
    *
    * @return Feature description used to identify the dependent feature in an evaluation dataset.
   */
  def dependentFeature: ColumnDef
  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[ClassificationSQLTransformer] = None
}

trait ClusteringRowModel extends CategoricalRowModel {
  def transformer: ClusteringTransformer
  def outputFeatures = FeatureUtil.clusteringOutputFeatures

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[ClusteringSQLTransformer] = None
}

trait RegressionRowModel extends RowModel {
  def transformer: RegressionTransformer
  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[RegressionSQLTransformer] = None

  def outputFeatures = FeatureUtil.regressionOutputFeatures
  /**
   * Used when we are doing model quality evaluation e.g. Confusion Matrix,
   * so we know what feature in the test dataset to compare the result to.
    *
    * @return Feature description used to identify the dependent feature in an evaluation dataset.
   */
  def dependentFeature: ColumnDef

}
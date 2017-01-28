/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.model._
import com.alpine.model.export.pfa.modelconverters.PipelinePFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.multiple.sql.{PipelineClassificationSQLTransformer, PipelineClusteringSQLTransformer, PipelineRegressionSQLTransformer, PipelineSQLTransformer}
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.RegressionSQLTransformer

/**
 * Used to combine models in sequence.
 * e.g.
 *  the output of one model is the input to the next.
 */
@SerialVersionUID(-5456055908821806710L)
case class PipelineRowModel(transformers: Seq[RowModel], override val identifier: String = "")
  extends RowModel with PFAConvertible {

  override def transformer = new PipelineTransformer(transformers.map(t => t.transformer).toList, transformers)

  @transient lazy val outputFeatures: Seq[ColumnDef] = transformers.last.transformationSchema.outputFeatures

  @transient lazy val inputFeatures: Seq[ColumnDef] = transformers.head.transformationSchema.inputFeatures

  @transient override lazy val sqlOutputFeatures: Seq[ColumnDef] = transformers.last.sqlOutputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ transformers.flatMap(t => t.classesForLoading).toSet
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator) = {
    PipelineSQLTransformer.make(this, sqlGenerator)
  }

  override def getPFAConverter: PFAConverter = new PipelinePFAConverter(transformers)

  override def streamline(requiredOutputFeatureNames: Seq[String]): PipelineRowModel = {
    val newModels = PipelineUtil.streamlineModelList(transformers, requiredOutputFeatureNames)
    PipelineUtil.validateModelList(newModels)
    PipelineRowModel(newModels, this.identifier)
  }
}

/**
  * Used for combining a Regression model (e.g. Linear Regression) with preprocessors (e.g. One Hot Encoding).
  */
@SerialVersionUID(381487725247733182L)
case class PipelineRegressionModel(preProcessors: Seq[RowModel], finalModel: RegressionRowModel, override val identifier: String = "")
  extends RegressionRowModel with PFAConvertible {

  override def transformer = {
    new PipelineRegressionTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer, preProcessors ++ List(finalModel))
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[RegressionSQLTransformer] = {
    PipelineRegressionSQLTransformer.make(this, sqlGenerator)
  }

  override def dependentFeature = finalModel.dependentFeature

  override def outputFeatures: Seq[ColumnDef] = finalModel.outputFeatures

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  @transient override lazy val sqlOutputFeatures: Seq[ColumnDef] = finalModel.sqlOutputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }

  override def getPFAConverter: PFAConverter = new PipelinePFAConverter(preProcessors ++ Seq(finalModel))

  override def streamline(requiredOutputFeatureNames: Seq[String]): PipelineRegressionModel = {
    val streamlinedFinalModel = finalModel.streamline(requiredOutputFeatureNames)
    val newPreprocessors = PipelineUtil.streamlineModelList(preProcessors, streamlinedFinalModel.inputFeatures.map(_.columnName))
    PipelineUtil.validateModelList(newPreprocessors ++ Seq(streamlinedFinalModel))
    PipelineRegressionModel(newPreprocessors, streamlinedFinalModel, this.identifier)
  }

}
/**
  * Used for combining a Clustering model (e.g. K-Means) with preprocessors (e.g. One Hot Encoding).
  */
@SerialVersionUID(-8221170007141359159L)
case class PipelineClusteringModel(preProcessors: Seq[RowModel], finalModel: ClusteringRowModel, override val identifier: String = "")
  extends ClusteringRowModel with PFAConvertible {

  override def transformer = {
    PipelineClusteringTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer, preProcessors ++ List(finalModel))
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator) = {
    PipelineClusteringSQLTransformer.make(this, sqlGenerator)
  }

  override def classLabels = finalModel.classLabels

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  @transient override lazy val sqlOutputFeatures: Seq[ColumnDef] = finalModel.sqlOutputFeatures

  override def outputFeatures = finalModel.outputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }

  override def getPFAConverter: PFAConverter = new PipelinePFAConverter(preProcessors ++ Seq(finalModel))

  override def streamline(requiredOutputFeatureNames: Seq[String]): PipelineClusteringModel = {
    val streamlinedFinalModel = finalModel.streamline(requiredOutputFeatureNames)
    val newPreprocessors = PipelineUtil.streamlineModelList(preProcessors, streamlinedFinalModel.inputFeatures.map(_.columnName))
    PipelineUtil.validateModelList(newPreprocessors ++ Seq(streamlinedFinalModel))
    PipelineClusteringModel(newPreprocessors, streamlinedFinalModel)
  }

}

/**
  * Used for combining a Classification model (e.g. Logistic Regression) with preprocessors (e.g. One Hot Encoding).
  */
@SerialVersionUID(-2407095602660767904L)
case class PipelineClassificationModel(preProcessors: Seq[RowModel], finalModel: ClassificationRowModel, override val identifier: String = "")
  extends ClassificationRowModel with PFAConvertible {

  override def transformer = {
    PipelineClassificationTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer, preProcessors ++ List(finalModel))
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator) = {
    PipelineClassificationSQLTransformer.make(this, sqlGenerator)
  }

  override def classLabels = finalModel.classLabels

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  @transient override lazy val sqlOutputFeatures: Seq[ColumnDef] = finalModel.sqlOutputFeatures

  override def outputFeatures = finalModel.outputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }

  // Used when we are doing model quality evaluation e.g. Confusion Matrix,
  override def dependentFeature = finalModel.dependentFeature

  override def getPFAConverter: PFAConverter = new PipelinePFAConverter(preProcessors ++ Seq(finalModel))

  override def streamline(requiredOutputFeatureNames: Seq[String]): PipelineClassificationModel = {
    val streamlinedFinalModel = finalModel.streamline(requiredOutputFeatureNames)
    val newPreprocessors = PipelineUtil.streamlineModelList(preProcessors, streamlinedFinalModel.inputFeatures.map(_.columnName))
    PipelineUtil.validateModelList(newPreprocessors ++ Seq(streamlinedFinalModel))
    PipelineClassificationModel(newPreprocessors, streamlinedFinalModel)
  }

}

object PipelineUtil {

  def streamlineModelList(models: Seq[RowModel], requiredOutputFeatureNames: Seq[String]): List[RowModel] = {
    var newModels: List[RowModel] = List()
    val reverseIterator = models.reverseIterator
    var currentNeededOutputNames = requiredOutputFeatureNames
    while(reverseIterator.hasNext) {
      val newModel = reverseIterator.next.streamline(currentNeededOutputNames)
      currentNeededOutputNames = newModel.inputFeatures.map(_.columnName)
      newModels = newModel :: newModels
    }
    newModels
  }

  /**
    * If the feature names don't match between layers, then the model will not be executable, so we don't want to generate that model.
    * @param models list of sequential models.
    */
  @throws(classOf[Exception])
  def validateModelList(models: Seq[RowModel]): Unit = {
    if (models.size < 2) {
      return
    }
    var index = 0
    while (index < models.size - 1) {
      val current = models(index)
      val next = models(index + 1)
      checkFeaturesAreAvailable(current.outputFeatures, next.inputFeatures)
      index += 1
    }
  }

  @throws(classOf[Exception])
  def checkFeaturesAreAvailable(previousOutputFeatures: Seq[ColumnDef], nextInputFeatures: Seq[ColumnDef]): Unit = {
    val previousOutputNames: Set[String] = previousOutputFeatures.map(c => c.columnName).toSet
    for (inputFeature <- nextInputFeatures) {
      if (!previousOutputNames.contains(inputFeature.columnName)) {
        throw new RuntimeException("Feature name " + inputFeature.columnName + " not found in output feature names " + previousOutputNames + " of previous model. " +
          "This is not a valid list of sequential models.")
      }
    }
  }
}

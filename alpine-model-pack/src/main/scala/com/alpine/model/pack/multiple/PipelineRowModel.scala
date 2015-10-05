/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.model.{CategoricalRowModel, ClassificationRowModel, RegressionRowModel, RowModel}
import com.alpine.plugin.core.io.ColumnDef

/**
 * Used to combine models in sequence.
 * e.g.
 *  the output of one model is the input to the next.
 */
case class PipelineRowModel(transformers: Seq[RowModel], override val identifier: String = "") extends RowModel {

  override def transformer = new PipelineTransformer(transformers.map(t => t.transformer).toList)

  @transient lazy val outputFeatures: Seq[ColumnDef] = transformers.last.transformationSchema.outputFeatures

  @transient lazy val inputFeatures: Seq[ColumnDef] = transformers.head.transformationSchema.inputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ transformers.flatMap(t => t.classesForLoading).toSet
  }

}

case class PipelineRegressionModel(preProcessors: Seq[RowModel], finalModel: RegressionRowModel, override val identifier: String = "") extends RegressionRowModel {

  override def transformer = new PipelineRegressionTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer)
  override def dependentFeature = finalModel.dependentFeature

  override def outputFeatures: Seq[ColumnDef] = finalModel.outputFeatures

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }
}

case class PipelineCategoricalModel(preProcessors: Seq[RowModel], finalModel: CategoricalRowModel, override val identifier: String = "") extends CategoricalRowModel {

  override def transformer = new PipelineCategoricalTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer)
  override def classLabels = finalModel.classLabels

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  override def outputFeatures = finalModel.outputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }
}

case class PipelineClassificationModel(preProcessors: Seq[RowModel], finalModel: ClassificationRowModel, override val identifier: String = "") extends ClassificationRowModel {

  override def transformer = new PipelineCategoricalTransformer(preProcessors.map(t => t.transformer).toList, finalModel.transformer)
  override def classLabels = finalModel.classLabels

  @transient lazy val inputFeatures: Seq[ColumnDef] = preProcessors.head.transformationSchema.inputFeatures

  override def outputFeatures = finalModel.outputFeatures

  override def classesForLoading = {
    super.classesForLoading ++ preProcessors.flatMap(t => t.classesForLoading).toSet ++ finalModel.classesForLoading
  }

  // Used when we are doing model quality evaluation e.g. Confusion Matrix,
  override def dependentFeature = finalModel.dependentFeature
}

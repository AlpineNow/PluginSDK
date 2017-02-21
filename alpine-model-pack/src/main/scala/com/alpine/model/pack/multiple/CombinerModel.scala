/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.modelconverters.CombinerPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.multiple.sql.CombinerSQLTransformer
import com.alpine.model.pack.preprocess.RenamingModel
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.util.{FilteredSeq, ModelUtil}

import scala.collection.mutable.ListBuffer

/**
  * Used to combine several models in parallel.
  * e.g.
  * the input features set is the (distinct) union of the input features of the sub-models.
  * the output features are the concatenation of the output features of the sub-models
  * (with string suffices added to ensure uniqueness of names).
  */
@SerialVersionUID(7750323944753065824L)
case class CombinerModel(models: Seq[ModelWithID], override val identifier: String = "")
  extends RowModel with PFAConvertible {

  override def transformer: Transformer = CombinerTransformer(this)

  // Make this a val so it will be included in the JSON.
  val inputFeatures: Seq[ColumnDef] = {
    // Combine input features but remove duplicates.
    models.flatMap(t => t.model.inputFeatures).distinct
  }

  @transient lazy val outputFeatures: Seq[ColumnDef] = {
    CombinerModel.getOutputFeaturesWithGroupID(models.map(t => (t.id, t.model.transformationSchema.outputFeatures)))
  }

  override def sqlOutputFeatures: Seq[ColumnDef] = {
    CombinerModel.getOutputFeaturesWithGroupID(models.map(m => (m.id, m.model.sqlOutputFeatures)))
  }

  override def classesForLoading: Set[Class[_]] = {
    super.classesForLoading ++ models.flatMap(t => t.model.classesForLoading).toSet
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[CombinerSQLTransformer] = {
    CombinerSQLTransformer.make(this, sqlGenerator)
  }

  override def getPFAConverter: PFAConverter = new CombinerPFAConverter(this)

  override def streamline(requiredOutputFeatureNames: Seq[String]): RowModel = {
    /*
      This is extremely complicated.

      First we have to track down which output features come from which sub-models.
      We calculate indicesToKeepFromOldModel, which is the list of indices of features in the main model output list that we need to keep.
      We use the cumulativeSum to work out the boundaries in main model output list, and call these "fence-posts",
      where the new sub-models output features start at every fence-post.
      */
    val indicesToKeepFromOldModel: Seq[Int] = requiredOutputFeatureNames.map(name => outputFeatures.indexWhere(c => c.columnName == name))
    val outputFeatureFencePosts = ModelUtil.cumulativeSum(this.models.map(m => m.model.outputFeatures.size))
    /*
      Filter the list of sub-models to keep only the ones who have output features that we want to keep
      inputModelsToKeep is a list of tuples, where each tuple contains the sub-model, and that sub-models
      index in the original list of models.
      */
    val inputModelsToKeep: Seq[(ModelWithID, Int)] = models.zipWithIndex.filter { case (model, i) =>
      // This model is responsible for output features in the main model list with indices j
      // such that min <= j < max.
      val (min, max) = (outputFeatureFencePosts(i), outputFeatureFencePosts(i + 1))
      indicesToKeepFromOldModel.exists(j => {
        min <= j && j < max
      })
    }
    /*
      Now we apply the streamline function to each of the models we want to keep, so they may drop redundant data and inputFeatures.
      The output features for the sub-model may not correspond directly to the output features in the main list,
      as we add the model id, and sometimes integer suffices to avoid naming collisions.
      So we deal with the features by index.
     */
    val streamlinedModels: Seq[(ModelWithID, Map[Int, Int])] = inputModelsToKeep.map {
      case (ModelWithID(id, model), index) =>
        val (min, max) = (outputFeatureFencePosts(index), outputFeatureFencePosts(index + 1))
        // Work out which of the features we want to keep come from this model.
        // i.e. those indicesToKeepFromOldModel which fall in the range for this sub-model, which is [min, max).
        // Then subtract min, to get the indices of these features from the sub-model's point of view.
        val neededOutputIndices = indicesToKeepFromOldModel.filter(j => {
          min <= j && j < max
        }).map(j => j - min)
        // Then work out the names of these features, as referenced by the sub-model.
        val needOutputColumnsForSubModel = neededOutputIndices.map(j => model.outputFeatures(j).columnName)
        // Apply the streamline function.
        val newSubModel = model.streamline(needOutputColumnsForSubModel)
        // Sub-model may still have more columns than we need. E.g. for one hot encoding.
        val outputIndicesInSubModel = needOutputColumnsForSubModel.map(name => newSubModel.outputFeatures.indexWhere(c => c.columnName == name))
        (ModelWithID(id, newSubModel), (neededOutputIndices.map(_ + min) zip outputIndicesInSubModel).toMap)
    }
    // This newModel is a CombinerModel of all the streamlined models.
    // However, the features in this model may not have the same output names as they had in the original model.
    val newModel = new CombinerModel(streamlinedModels.unzip._1, this.identifier)
    val newOutputFencePosts = ModelUtil.cumulativeSum(streamlinedModels.map(_._1.model.outputFeatures.size))

    // This is a map of indices of the required output features in the old model,
    // to the indices of those same features in the new model.
    // The keys in this map should be equal to indicesToKeepFromOldModel.
    val oldIndexToNewIndexMap: Map[Int, Int] = streamlinedModels.zipWithIndex.flatMap {
      case ((_, modelIndexMap), indexOfModel) => modelIndexMap.map {
        case (oldIndex, indexInSubModel) => (oldIndex, indexInSubModel + newOutputFencePosts(indexOfModel))
      }
    }.toMap

    // Rename the output features of the new CombinerModel to the names that we need.
    val renaming = RenamingModel(
      inputFeatures = FilteredSeq(newModel.outputFeatures, indicesToKeepFromOldModel.map(i => oldIndexToNewIndexMap(i))),
      outputNames = requiredOutputFeatureNames
    )
    // If the renaming is trivial (no names actually change), then drop it.
    if (renaming.isRedundant) {
      newModel
    } else {
      PipelineRowModel(Seq(newModel, renaming), this.identifier)
    }
  }

}

case class ModelWithID(id: String, model: RowModel)

case class CombinerTransformer(model: CombinerModel) extends Transformer {

  private val scorersWithIndices: Seq[(Transformer, Array[Int])] = {
    val transformers = model.models.map(t => t.model)
    val inputFeatureDescs = model.inputFeatures
    transformers.map(t => (
      t.transformer, t.transformationSchema.inputFeatures.map(f => inputFeatureDescs.indexOf(f)).toArray)
    )
  }

  def apply(row: Row): Seq[Any] = {
    scorersWithIndices.flatMap {
      case (transformer, indices) => transformer.apply(FilteredSeq(row, indices))
    }
  }

}

object CombinerModel {

  val sepChar = '_'

  def getOutputFeatures(subOutputFeatures: Seq[Seq[ColumnDef]]): Seq[ColumnDef] = {
    val featureBuilder = ListBuffer[ColumnDef]()
    for (subFeatures <- subOutputFeatures) {
      val suffix = getSuffixForConcatenation(featureBuilder, subFeatures)
      for (feature <- subFeatures) {
        featureBuilder.append(ColumnDef(feature.columnName + suffix, feature.columnType))
      }
    }
    featureBuilder
  }

  def getOutputFeaturesWithGroupID(subOutputFeatures: Seq[(String, Seq[ColumnDef])]): Seq[ColumnDef] = {
    val featureBuilder = ListBuffer[ColumnDef]()
    for (subFeatures <- subOutputFeatures) {
      val subFeatureDescs = subFeatures._2
      val groupID = subFeatures._1
      val suffix = getSuffixForConcatenation(featureBuilder, subFeatureDescs, groupID)
      for (feature <- subFeatureDescs) {
        featureBuilder.append(ColumnDef(feature.columnName + suffix, feature.columnType))
      }
    }
    featureBuilder
  }

  private def getSuffixForConcatenation(featuresSoFar: ListBuffer[ColumnDef], newFeatures: Seq[ColumnDef], groupID: String = ""): String = {
    val groupIDWithUnderscore = if (groupID == "") groupID else sepChar + groupID
    var suffix = groupIDWithUnderscore
    var count = 0
    val featureNamesSoFar = featuresSoFar.map(f => f.columnName)
    // Important to match just on the name, not on the whole ColumnDef,
    // as columns with the same name but different types are still illegal.
    while (newFeatures.map(s => s.columnName + suffix).intersect(featureNamesSoFar).nonEmpty) {
      count += 1
      suffix = groupIDWithUnderscore + sepChar + count.toString
    }
    suffix
  }

  // Similar to apply, but we don't call it "apply" because
  // then the constructors can't be distinguished after type erasure.
  def make(models: Seq[RowModel]): CombinerModel = {
    new CombinerModel(models.map(m => ModelWithID(m.identifier, m)))
  }

  def make[T <: RowModel](models: java.util.List[T]): CombinerModel = {
    import scala.collection.JavaConversions._
    new CombinerModel(models.map(m => ModelWithID(m.identifier, m)))
  }

}

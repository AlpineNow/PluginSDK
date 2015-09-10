/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.features.FeatureDesc
import com.alpine.model.RowModel
import com.alpine.transformer.Transformer

import scala.collection.mutable.ListBuffer

/**
 * Used to combine several models in parallel.
 * e.g.
 * the input features set is the (distinct) union of the input features of the sub-models.
 * the output features are the concatenation of the output features of the sub-models
 *  (with string suffices added to ensure uniqueness of names).
 */
case class CombinerModel(models: Seq[ModelWithID], override val identifier: String = "") extends RowModel {

  override def transformer: Transformer = CombinerTransformer(this)

  // Make this a val so it will be included in the JSON.
  val inputFeatures: Seq[FeatureDesc[_]] = {
    // Combine input features but remove duplicates.
    models.flatMap(t => t.model.transformationSchema.inputFeatures).distinct
  }

  def outputFeatures: Seq[FeatureDesc[_]] = {
    CombinerModel.getOutputFeaturesWithGroupID(models.map(t => (t.id, t.model.transformationSchema.outputFeatures)))
  }

  override def classesForLoading = {
    super.classesForLoading ++ models.flatMap(t => t.model.classesForLoading).toSet
  }
}

case class ModelWithID(id: String, model: RowModel)

case class CombinerTransformer(combinerTransformer: CombinerModel) extends Transformer {

  private val scorersWithIndices: Seq[(Transformer, Array[Int])] = {
    val transformers = combinerTransformer.models.map(t => t.model)
    val inputFeatureDescs = combinerTransformer.inputFeatures
    transformers.map(t => (
      t.transformer, t.transformationSchema.inputFeatures.map(f => inputFeatureDescs.indexOf(f)).toArray)
    )
  }

  def apply(row: Row) = {
    scorersWithIndices.flatMap(x => x._1.apply(FilteredSeq(row, x._2)))
  }

}

object CombinerModel {

  val sepChar = '_'

  def getOutputFeatures(subOutputFeatures: Seq[Seq[FeatureDesc[_]]]): Seq[FeatureDesc[_]] = {
    val featureBuilder = ListBuffer[FeatureDesc[_]]()
    for (subFeatures <- subOutputFeatures) {
      val suffix = getSuffixForConcatenation(featureBuilder, subFeatures)
      for (feature <- subFeatures) {
        featureBuilder.append(new FeatureDesc(feature.name + suffix, feature.dataType))
      }
    }
    featureBuilder.toSeq
  }

  def getOutputFeaturesWithGroupID(subOutputFeatures: Seq[(String, Seq[FeatureDesc[_]])]): Seq[FeatureDesc[_]] = {
    val featureBuilder = ListBuffer[FeatureDesc[_]]()
    for (subFeatures <- subOutputFeatures) {
      val subFeatureDescs = subFeatures._2
      val groupID = subFeatures._1
      val suffix = getSuffixForConcatenation(featureBuilder, subFeatureDescs, groupID)
      for (feature <- subFeatureDescs) {
        featureBuilder.append(new FeatureDesc(feature.name + suffix, feature.dataType))
      }
    }
    featureBuilder.toSeq
  }

  private def getSuffixForConcatenation(featuresSoFar: ListBuffer[FeatureDesc[_]], newFeatures: Seq[FeatureDesc[_]], groupID: String = ""): String = {
    val groupIDWithUnderscore = if (groupID == "") groupID else sepChar + groupID
    var suffix = groupIDWithUnderscore
    var count = 0
    val featureNamesSoFar = featuresSoFar.map(f => f.name)
    while (newFeatures.map(s => s.name + suffix).intersect(featureNamesSoFar).nonEmpty) {
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

}

case class FilteredSeq[A](originalSeq: Seq[A], indicesToUse: Seq[Int]) extends Seq[A] {
  override def length: Int = indicesToUse.length

  override def apply(idx: Int): A = originalSeq(indicesToUse(idx))

  override def iterator: Iterator[A] = {
    new Iterator[A] {
      val indexIterator = indicesToUse.iterator

      override def hasNext: Boolean = indexIterator.hasNext

      override def next(): A = originalSeq(indexIterator.next())
    }
  }
}



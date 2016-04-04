/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.model.RowModel
import com.alpine.result._
import com.alpine.transformer._
import com.alpine.util.FilteredSeq

import scala.collection.mutable.ListBuffer

/**
 * Transformer to apply several transformers in sequence.
 * The output of each sub-transformer is used as input to the next,
 * and the output of the final sub-transformer is the final output.
 */
class PipelineTransformer(transformers: List[Transformer], subModels: Seq[RowModel]) extends Transformer {

  protected val reorderingIndices: List[Option[Array[Int]]] = {
    var i = 1
    val indicesSoFar = new ListBuffer[Option[Array[Int]]]
    indicesSoFar.append(None) // Always start with the correct order.
    while (i < subModels.length) {
      val previousOutputFeatures = subModels(i - 1).outputFeatures
      val currentInputFeatures = subModels(i).inputFeatures
      if (previousOutputFeatures == currentInputFeatures) {
        indicesSoFar.append(None)
      } else {
        val newIndices = currentInputFeatures.map(inputColumn => {
          previousOutputFeatures.indexWhere(outputColumn => outputColumn.columnName == inputColumn.columnName)
        }).toArray
        indicesSoFar.append(Some(newIndices))
      }
      i+= 1
    }
    indicesSoFar.toList
  }

  protected val lastReordering = reorderingIndices.last

  override def apply(row: Row): Row = {
    apply(row, transformers, reorderingIndices)
  }

  protected def apply(row: Row, tList: List[Transformer], reordering: List[Option[Array[Int]]]): Row = {
    tList match {
      case Nil => row // Have run out of transformations, so do no-op.
      case _ => if (reordering.head.isEmpty) {
        apply(tList.head.apply(row), tList.tail, reordering.tail)
      } else {
        apply(tList.head.apply(FilteredSeq(row, reordering.head.get)), tList.tail, reordering.tail)
      }
    }
  }
}

class PipelineMLTransformer[R <: MLResult](preProcessors: List[_ <: Transformer], finalTransformer: MLTransformer[R], subModels: Seq[RowModel])
  extends PipelineTransformer(preProcessors ++ List(finalTransformer), subModels) with MLTransformer[R] {

  override def score(row: Row): R = {
    finalTransformer.score(inputRowForLastModel(row))
  }

  protected def inputRowForLastModel(row: Row): Row = {
    if (lastReordering.isEmpty) {
      apply(row, preProcessors, reorderingIndices)
    } else {
      FilteredSeq(apply(row, preProcessors, reorderingIndices), lastReordering.get)
    }
  }

}
class PipelineRegressionTransformer[R <: MLResult](preProcessors: List[_ <: Transformer], finalTransformer: RegressionTransformer, subModels: Seq[RowModel])
  extends PipelineMLTransformer(preProcessors, finalTransformer, subModels) with RegressionTransformer {

  override def predict(row: Row): Double = {
    finalTransformer.predict(inputRowForLastModel(row))
  }

  override def apply(row: Row): Row = {
    finalTransformer.apply(inputRowForLastModel(row))
  }

  override def score(row: Row): RealResult = {
    RealResult(predict(row))
  }
}

class PipelineCategoricalTransformer[R <: CategoricalResult](preProcessors: List[_ <: Transformer], finalTransformer: CategoricalTransformer[R], subModels: Seq[RowModel])
  extends PipelineMLTransformer[R](preProcessors, finalTransformer, subModels) with CategoricalTransformer[R] {

  override def apply(row: Row): Row = {
    finalTransformer.apply(inputRowForLastModel(row))
  }

  /**
   * The result must always return the labels in the order specified here.
    *
    * @return The class labels in the order that they will be returned by the result.
   */
  override def classLabels: Seq[String] = finalTransformer.classLabels
}

case class PipelineClassificationTransformer(preProcessors: List[_ <: Transformer], finalTransformer: ClassificationTransformer, subModels: Seq[RowModel])
  extends PipelineCategoricalTransformer(preProcessors, finalTransformer, subModels) with ClassificationTransformer {
  override def scoreConfidences(row: Row): Array[Double] = {
    finalTransformer.scoreConfidences(inputRowForLastModel(row))
  }

  override def score(row: Row): ClassificationResult = {
    finalTransformer.score(inputRowForLastModel(row))
  }
}

case class PipelineClusteringTransformer(preProcessors: List[_ <: Transformer], finalTransformer: ClusteringTransformer, subModels: Seq[RowModel])
  extends PipelineCategoricalTransformer(preProcessors, finalTransformer, subModels) with ClusteringTransformer {
  override def scoreDistances(row: Row): Array[Double] = {
    finalTransformer.scoreDistances(inputRowForLastModel(row))
  }

  override def score(row: Row): ClusteringResult = {
    finalTransformer.score(inputRowForLastModel(row))
  }
}

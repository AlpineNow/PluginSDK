/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.multiple

import com.alpine.result.{CategoricalResult, MLResult}
import com.alpine.transformer.{CategoricalTransformer, MLTransformer, RegressionTransformer, Transformer}

/**
 * Transformer to apply several transformers in sequence.
 * The output of each sub-transformer is used as input to the next,
 * and the output of the final sub-transformer is the final output.
 */
class PipelineTransformer(transformers: List[Transformer]) extends Transformer {

  def this(transformerArgs: Transformer*) {
    this(transformerArgs.toList)
  }

  override def apply(row: Row): Row = {
    apply(row, transformers)
  }

  // TODO: Allow reordering of features between models.
  protected def apply(row: Row, tList: List[Transformer]): Row = {
    tList match {
      case Nil => row // Have run out of transformations, so do no-op.
      case _ => apply(tList.head.apply(row), tList.tail)
    }
  }
}

class PipelineMLTransformer[R <: MLResult](preProcessors: List[_ <: Transformer], finalTransformer: MLTransformer[R])
  extends PipelineTransformer(preProcessors ++ List(finalTransformer)) with MLTransformer[R] {

  override def score(row: Row): R = {
    finalTransformer.score(apply(row, preProcessors))
  }

}
class PipelineRegressionTransformer[R <: MLResult](preProcessors: List[_ <: Transformer], finalTransformer: RegressionTransformer)
  extends PipelineTransformer(preProcessors ++ List(finalTransformer)) with RegressionTransformer {
  override def predict(row: Row): Double = {
    finalTransformer.predict(apply(row, preProcessors))
  }

  override def apply(row: Row): Row = {
    finalTransformer.apply(apply(row, preProcessors))
  }
}

case class PipelineCategoricalTransformer[R <: CategoricalResult](preProcessors: List[_ <: Transformer], finalTransformer: CategoricalTransformer[R])
  extends PipelineMLTransformer[R](preProcessors, finalTransformer) with CategoricalTransformer[R] {

  override def apply(row: Row): Row = {
    finalTransformer.apply(apply(row, preProcessors))
  }

  /**
   * The result must always return the labels in the order specified here.
   * @return The class labels in the order that they will be returned by the result.
   */
  override def classLabels: Seq[String] = finalTransformer.classLabels
}

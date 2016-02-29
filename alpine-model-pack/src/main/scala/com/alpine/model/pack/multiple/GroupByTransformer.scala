/*
 * COPYRIGHT (C) Feb 12 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple

import com.alpine.model.pack.util.TransformerUtil
import com.alpine.model.{ClassificationRowModel, RegressionRowModel, RowModel}
import com.alpine.plugin.core.io.ColumnType
import com.alpine.transformer.{ClassificationTransformer, RegressionTransformer}
import com.alpine.util.FilteredSeq

/**
  * Created by Jennifer Thompson on 2/12/16.
  */
trait GroupByTransformer[M <: RowModel] {

  def model: GroupByModel[M]

  lazy val getGroupByValue: (Any => Any) = {
    if (model.groupByFeature.columnType == ColumnType.Double) {
      TransformerUtil.anyToDouble
    } else if (model.groupByFeature.columnType == ColumnType.Long) {
      x => TransformerUtil.anyToDouble(x).toLong
    } else if (model.groupByFeature.columnType == ColumnType.Int) {
      x => TransformerUtil.anyToDouble(x).toInt
    } else if (model.groupByFeature.columnType == ColumnType.Float) {
      x => TransformerUtil.anyToDouble(x).toFloat
    } else if (model.groupByFeature.columnType == ColumnType.String) {
      x => x.toString
    } else {
      x => x
    }
  }

  def indicesToUse(subModel: M): Array[Int] = {
    val completeInputFeatures = model.inputFeatures
    subModel.inputFeatures.map(feature => completeInputFeatures.indexOf(feature)).toArray
  }

}

case class GroupByRegressionTransformer(model: GroupByRegressionModel)
  extends RegressionTransformer with GroupByTransformer[RegressionRowModel] {

  private val scorersWithIndices = {
    model.modelsByGroup.map { case (groupByValue, subModel) =>
      (groupByValue, (subModel.transformer, indicesToUse(subModel)))
    }
  }

  override def predict(row: Row): Double = {
    val groupByValue = getGroupByValue(row.head)
    scorersWithIndices.get(groupByValue) match {
      case Some((scorer, indices)) => scorer.predict(FilteredSeq(row, indices))
      case None =>  Double.NaN
    }
  }
}

case class GroupByClassificationTransformer(model: GroupByClassificationModel)
  extends ClassificationTransformer with GroupByTransformer[ClassificationRowModel] {

  private val scorersWithIndices = {
    model.modelsByGroup.map { case (groupByValue, subModel) =>
      (groupByValue, (subModel.transformer, indicesToUse(subModel)))
    }
  }

  private val confidenceReordering: Map[Any, Seq[Int]] = {
    scorersWithIndices.flatMap { case (groupByValue, (transformer, _)) =>
      val subClassLabels: Seq[String] = transformer.classLabels
      if (subClassLabels == classLabels) {
        None
      } else {
        // classLabels is a superset of subClassLabels, by construction.
        Some(groupByValue, subClassLabels.map(l => classLabels.indexOf(l)))
      }
    }
  }

  override def classLabels: Seq[String] = model.classLabels

  override def scoreConfidences(row: Row): Array[Double] = {
    val groupByValue = getGroupByValue(row.head)
    scorersWithIndices.get(groupByValue) match {
      case Some((scorer, indices)) =>
        reorderConfidences(groupByValue, scorer.scoreConfidences(FilteredSeq(row, indices)))
      case None => null
    }
  }

  private def reorderConfidences(groupByValue: Any, rawConfidences: Array[Double]): Array[Double] = {
    val reordering = confidenceReordering.get(groupByValue)
    reordering match {
      case Some(confIndices) =>
        val output = Array.ofDim[Double](classLabels.length) // Initializes to 0 by default.
        var i = 0
        while (i < rawConfidences.length) {
          output(confIndices(i)) = rawConfidences(i)
          i += 1
        }
        output
      case None => rawConfidences
    }
  }
}

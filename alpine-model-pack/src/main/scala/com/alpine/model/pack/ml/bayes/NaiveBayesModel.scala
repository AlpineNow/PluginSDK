/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ml.bayes

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.ClassificationRowModel
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.{AliasGenerator, SQLGenerator}
import com.alpine.transformer.ClassificationTransformer
import com.alpine.transformer.sql.{ClassificationModelSQLExpressions, ClassificationSQLTransformer, ColumnName, ColumnarSQLExpression}
import com.alpine.util.SQLUtility

/**
  * Created by Jennifer Thompson on 7/7/16.
  */

case class NaiveBayesModel(inputFeatures: Seq[ColumnDef],
                           dependentFeatureName: String,
                           distributions: Seq[Distribution],
                           threshold: Double) extends ClassificationRowModel {

  override def transformer: ClassificationTransformer = new NaiveBayesTransformer(this)

  override def dependentFeature: ColumnDef = ColumnDef(dependentFeatureName, ColumnType.String)

  override def classLabels: Seq[String] = distributions.map(_.classLabel)

  override def sqlTransformer(sqlGenerator: SQLGenerator) = {
    Some(new NaiveBayesSQLTransformer(this, sqlGenerator))
  }
}

trait BayesLikelihood

case class GaussianLikelihood(mu: Double, sigma: Double) extends BayesLikelihood {
  def likelihood(d: Double): Double = {
    if (sigma != 0) {
      val diff = d - mu
      math.exp(-(diff * diff) / (2 * sigma * sigma)) / sigma
    } else if (d == mu) {
      // It's preferable to avoid this by never creating a model with 0 as a sigma value
      // (just pick some close to 0 value instead or remove the feature),
      // as this approach is a bit hacky.
      1
    } else {
      0
    }
    // leave out the root(2*pi) because it is a constant multiplier.
  }
}

case class CategoryCount(category: String, prob: Double)

case class CategoricalLikelihood(counts: Seq[CategoryCount]) extends BayesLikelihood {

  @transient lazy val map: Map[String, Double] = (counts.map(_.category) zip normalizedProbabilities).toMap

  private def normalizedProbabilities: Array[Double] = {
    val totalCount = counts.map(_.prob).sum
    counts.map(_.prob / totalCount).toArray
  }
}

case class Distribution(classLabel: String, priorProbability: Double, likelihoods: Seq[TypeWrapper[BayesLikelihood]]) {

  def score(row: Seq[Any], threshold: Double): Double = {
    var start = priorProbability
    var i = 0
    while (i < row.length) {
      val value = row(i)
      if (value != null) { // Skip feature if value is null. This is natural for Naive Bayes.
      val multiplier = likelihoods(i) match {
        case TypeWrapper(g: GaussianLikelihood) => g.likelihood(TransformerUtil.anyToDouble(value))
        case TypeWrapper(c: CategoricalLikelihood) => c.map.getOrElse(value.toString, 0d) // If unseen in training set, then it counts as 0 prob.
      }
        start *= math.max(multiplier, threshold)
      }
      i += 1
    }
    start
  }

}

class NaiveBayesTransformer(model: NaiveBayesModel) extends ClassificationTransformer {

  override def classLabels: Seq[String] = model.classLabels

  override def scoreConfidences(row: Row): Array[Double] = {
    TransformerUtil.normalizeProbabilities(model.distributions.map(d => d.score(row, model.threshold))).toArray
  }

  override def allowNullValues: Boolean = true
}

class NaiveBayesSQLTransformer(val model: NaiveBayesModel, sqlGenerator: SQLGenerator) extends ClassificationSQLTransformer {
  override def getClassificationSQL: ClassificationModelSQLExpressions = {

    val aliasGenerator = new AliasGenerator("conf")
    val labelValuesToColumnNames = model.distributions.map(d => (d.classLabel, ColumnName(aliasGenerator.getNextAlias))).toMap

    val unNormalizedConfs: Seq[(ColumnarSQLExpression, ColumnName)] = model.distributions.map {
      case Distribution(classLabel: String, priorProbability: Double, likelihoods: Seq[TypeWrapper[BayesLikelihood]]) => {
        val totalMultiplier = (this.inputColumnNames zip likelihoods.map(_.value)).map {
          case (columnName, likelihood) =>
            val quotedFeatureName = columnName.escape(sqlGenerator)
            // Null values go to 1, equivalent to skipping the feature.
            val whenNullThen1: String = s"""WHEN $quotedFeatureName IS NULL THEN 1"""
            likelihood match {
              case GaussianLikelihood(mu, sigma) =>
                if (sigma != 0) {
                  val sigmaSq2 = 2 * sigma * sigma
                  val expExpression = s"""EXP(-($quotedFeatureName - $mu)*($quotedFeatureName - $mu) / $sigmaSq2) / $sigma"""
                  s"""CASE $whenNullThen1 ELSE $expExpression END"""
                } else {
                  s"""CASE $whenNullThen1 WHEN $quotedFeatureName = $mu THEN 1 ELSE ${model.threshold} END"""
                }
              case c: CategoricalLikelihood =>
                val innards = c.map.map {
                  case (category: String, prob: Double) =>
                    s"""WHEN $quotedFeatureName = ${SQLUtility.wrapInSingleQuotes(category)} THEN $prob"""
                }.mkString(" ")
                // Null values go to 1, unseen values go to model.threshold (i.e. rare probability in training set).
                s"""CASE $whenNullThen1 $innards ELSE ${model.threshold} END"""
            }
        }.map(s => s"($s)")

        (ColumnarSQLExpression(priorProbability + " * " + totalMultiplier.mkString(" * ")), labelValuesToColumnNames(classLabel))
      }
    }
    val sumName: ColumnName = ColumnName("sum")
    val unNormalizedConfNames: Seq[ColumnName] = unNormalizedConfs.unzip._2
    val confsWithSum = unNormalizedConfNames.map(n => (n.asColumnarSQLExpression(sqlGenerator), n)) ++ Seq((ColumnarSQLExpression(unNormalizedConfNames.map(_.escape(sqlGenerator)).mkString(" + ")), sumName))
    val normalizedConfs = unNormalizedConfNames.map(_.escape(sqlGenerator) + " / " + sumName.escape(sqlGenerator)).map(ColumnarSQLExpression) zip unNormalizedConfNames
    ClassificationModelSQLExpressions(labelValuesToColumnNames.toSeq, Seq(unNormalizedConfs, confsWithSum, normalizedConfs), sqlGenerator)
  }

}
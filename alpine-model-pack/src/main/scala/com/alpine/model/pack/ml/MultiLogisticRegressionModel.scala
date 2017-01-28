/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.model.ClassificationRowModel
import com.alpine.model.export.pfa.modelconverters.LogisticRegressionPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.ClassificationTransformer
import com.alpine.transformer.sql._

/**
 * @author Jenny Thompson
 *         6/11/15
 */
@SerialVersionUID(1381676143872694894L)
case class MultiLogisticRegressionModel(singleLORs: Seq[SingleLogisticRegression],
                                      baseValue: String,
                                      dependentFeatureName: String,
                                      inputFeatures: Seq[ColumnDef],
                                      override val identifier: String = "")
  extends ClassificationRowModel with PFAConvertible {
  override def transformer = LogisticRegressionTransformer(this)

  @transient lazy val classLabels: List[String] = (singleLORs.map(l => l.dependentValue) ++ List(baseValue)).toList

  override def dependentFeature = new ColumnDef(dependentFeatureName, ColumnType.String)

  override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(LogisticRegressionSQLTransformer(this, sqlGenerator))

  override def getPFAConverter: PFAConverter = new LogisticRegressionPFAConverter(this)
}

/**
 * Represents a SingleLogisticRegression to be used as one of several in a MultiLogisticRegressionModel.
 *
 * We use java.lang.Double for the type of the numeric values, because the scala Double type information
 * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
 *
 * @param dependentValue The dependent value that the coefficients correspond to.
 * @param coefficients The coefficients for the single Logistic Regression model.
 * @param bias The constant term, that is added to the dot product of the feature coefficient vectors.
 */
case class SingleLogisticRegression(dependentValue: String, coefficients: Seq[java.lang.Double], bias: Double = 0)

case class LogisticRegressionTransformer(model: MultiLogisticRegressionModel) extends ClassificationTransformer {

  private val betaArrays = model.singleLORs.map(l => (l.bias, TransformerUtil.javaDoubleSeqToArray(l.coefficients))).toArray
  private val numBetaArrays = betaArrays.length

  // Reuse of this means that the scoring method is not thread safe.
  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)

  override def scoreConfidences(row: Row): Array[Double] = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    calculateConfidences(rowAsDoubleArray)
  }

  def calculateConfidences(x: Array[Double]): Array[Double] = {
    val p = Array.ofDim[Double](numBetaArrays + 1)
    val rowSize: Int = x.length
    p(numBetaArrays) = 1.0
    var z: Double = 1.0
    var i1: Int = 0
    while (i1 < numBetaArrays) {
      var tmp: Double = betaArrays(i1)._1
      val beta: Array[Double] = betaArrays(i1)._2
      var j: Int = 0
      while (j < rowSize) {
        tmp += x(j) * beta(j)
        j += 1
      }
      p(i1) = Math.exp(tmp)
      z += p(i1)
      i1 += 1
    }
    // Normalize
    var i2: Int = 0
    while (i2 < numBetaArrays + 1) {
      p(i2) /= z
      i2 += 1
    }
    p
  }

  /**
   * The result must always return the labels in the order specified here.
 *
   * @return The class labels in the order that they will be returned by the result.
   */
  override def classLabels: Seq[String] = model.classLabels

}

case class LogisticRegressionSQLTransformer(model: MultiLogisticRegressionModel, sqlGenerator: SQLGenerator)
  extends ClassificationSQLTransformer {

  def getClassificationSQL: ClassificationModelSQLExpressions = {

    val exponentialExpressions = getExponentials(sqlGenerator)
    /**
      * First layer, e.g.
      * e0 = EXP(4.0 + "x1" * 2.0 + "x2" * 3.0)
      * e1 = EXP(-3.2 + "x1" * 12 + "x2" * 4.0)
      * ...
      * en = ...
      */
    val firstLayer: Seq[(ColumnarSQLExpression, ColumnName)] = exponentialExpressions.zipWithIndex.map {
      case (expression, index) => (expression, ColumnName("e" + index))
    }

    /**
      * Second layer:
      * sum = 1 + e0 + e1 + ... + en
      * e0, e1, ... , en as carry-over columns.
      */
    val sum = ColumnarSQLExpression(
      s"""1 + ${
        firstLayer.map {
          case (_, expression) => expression.escape(sqlGenerator)
        }.mkString(" + ")
      }"""
    )
    val sumColumnName = ColumnName("sum")

    val secondLayer = (sum, sumColumnName) ::
      firstLayer.map {
        case (_, name) => (name.asColumnarSQLExpression(sqlGenerator), name)
      }.toList

    /**
      * Third layer
      * baseVal = 1 / "sum" [Confidence that the base label value is the correct one]
      * ce0 = e0 / sum [Confidence that the 0th label value is the correct one]
      * ce1 = e1 / sum [Confidence that the 1st label value is the correct one]
      * ...
      * cen = en / sum [Confidence that the nth label value is the correct one]
      */
    val baseValueConfColumnName: ColumnName = ColumnName("baseVal")
    val thirdLayer = (ColumnarSQLExpression( s"""1 / ${sumColumnName.escape(sqlGenerator)}"""), baseValueConfColumnName) ::
      firstLayer.map {
        case (_, columnName) =>
          (
            ColumnarSQLExpression( s"""${columnName.escape(sqlGenerator)} / ${sumColumnName.escape(sqlGenerator)}"""),
            ColumnName("c" + columnName.rawName)
          )
      }.toList

    /**
      * Have to know which confidence expression corresponds to each label value.
      */
    val labelValuesToColumnNames = (model.baseValue, baseValueConfColumnName) ::
      thirdLayer.tail.zipWithIndex.map {
        case (_, index) => (model.singleLORs(index).dependentValue, ColumnName("ce" + index))
      }

    val layers = Seq(firstLayer, secondLayer, thirdLayer)

    /**
      * Return a compound object that can be used for many things,
      * e.g.
      * ROC, Multi-class prediction, binary prediction.
      */
    ClassificationModelSQLExpressions(labelValuesToColumnNames, layers, sqlGenerator)
  }

  private def getExponentials(sqlGenerator: SQLGenerator): Seq[ColumnarSQLExpression] = {
    model.singleLORs
      .map(l =>
        LinearRegressionModel(l.coefficients, inputFeatures, l.bias)
          .sqlTransformer(sqlGenerator).get
          .predictionExpression
      ).map(s => ColumnarSQLExpression( s"""EXP($s)"""))
  }

}

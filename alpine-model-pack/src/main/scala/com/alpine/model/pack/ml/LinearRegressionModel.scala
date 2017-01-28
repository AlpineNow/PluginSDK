/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.model.RegressionRowModel
import com.alpine.model.export.pfa.modelconverters.LinearRegressionPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.ml.sql.LinearRegressionSQLTransformer
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.util.FilteredSeq

/**
  * Representation of the classical linear regression model
  * y = intercept + a_0 x_0 + a_1 x_1 + ... + a_n x_n.
  *
  * The length of the coefficients must match the length of the inputFeatures.
  *
  * We use java.lang.Double for the type of the coefficients, because the scala Double type information
  * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
  *
  * @param coefficients         Vector of coefficients of the model. Must match the length of inputFeatures.
  * @param inputFeatures        Description of the (numeric) input features. Must match the length of coefficients.
  * @param intercept            Intercept of the model (defaults to 0).
  * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
  * @param identifier           Used to identify this model when in a collection of models. Should be simple characters,
  *                             so it can be used in a feature name.
  */
@SerialVersionUID(-4888344570084962586L)
case class LinearRegressionModel(coefficients: Seq[java.lang.Double],
                                 inputFeatures: Seq[ColumnDef],
                                 intercept: Double = 0,
                                 dependentFeatureName: String = "",
                                 override val identifier: String = "")
  extends RegressionRowModel with PFAConvertible {

  override def transformer = new LinearRegressionTransformer(coefficients, intercept)

  override def dependentFeature = ColumnDef(dependentFeatureName, ColumnType.Double)

  override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(new LinearRegressionSQLTransformer(this, sqlGenerator))

  override def getPFAConverter: PFAConverter = LinearRegressionPFAConverter(this)

  override def streamline(requiredOutputFeatureNames: Seq[String]): RegressionRowModel = {
    // Ignore the requiredOutputFeatureNames, as a regression model always produces the same output.
    // However, we can drop any input features that are not using (as they have coefficient zero).
    val nonZeroCoefficientIndices = this.coefficients.indices.filter(i => this.coefficients(i) != 0)
    LinearRegressionModel(
      FilteredSeq(this.coefficients, nonZeroCoefficientIndices),
      FilteredSeq(this.inputFeatures, nonZeroCoefficientIndices),
      this.intercept,
      this.dependentFeatureName,
      this.identifier
    )
  }
}


object LinearRegressionModel {

  /**
    * An alternative constructor for LinearRegressionModel.
    *
    * Call this "make" instead of "apply" because the constructors need to be distinguishable after type erasure.
    *
    * @param coefficients         Vector of coefficients of the model. Must match the length of inputFeatures.
    * @param inputFeatures        Description of the (numeric) input features. Must match the length of coefficients.
    * @param intercept            Intercept of the model (defaults to 0).
    * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
    * @param identifier           Used to identify this model when in a collection of models. Should be simple characters,
    *                             so it can be used in a feature name.
    */
  def make(coefficients: Seq[Double],
           inputFeatures: Seq[ColumnDef],
           intercept: Double = 0,
           dependentFeatureName: String = "", identifier: String = ""): LinearRegressionModel = {
    LinearRegressionModel(
      TransformerUtil.toJavaDoubleSeq(coefficients),
      inputFeatures,
      intercept,
      dependentFeatureName,
      identifier
    )
  }
}
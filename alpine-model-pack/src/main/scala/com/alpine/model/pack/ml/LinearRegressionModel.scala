/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.features.{DoubleType, FeatureDesc}
import com.alpine.model.RegressionRowModel
import com.alpine.model.pack.util.TransformerUtil

/**
 * Representation of the classical linear regression model
 * y = intercept + a_0 x_0 + a_1 x_1 + ... + a_n x_n.
 *
 * The length of the coefficients must match the length of the inputFeatures.
 *
 * We use java.lang.Double for the type of the coefficients, because the scala Double type information
 * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
 *
 * @param coefficients Vector of coefficients of the model. Must match the length of inputFeatures.
 * @param inputFeatures Description of the (numeric) input features. Must match the length of coefficients.
 * @param intercept Intercept of the model (defaults to 0).
 * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
 * @param identifier Used to identify this model when in a collection of models. Should be simple characters,
 *                   so it can be used in a feature name.
 */
case class LinearRegressionModel(coefficients: Seq[java.lang.Double],
                                 inputFeatures: Seq[FeatureDesc[_ <: Number]],
                                 intercept: Double = 0,
                                 dependentFeatureName: String = "",
                                 override val identifier: String = "") extends RegressionRowModel {

  override def transformer = new LinearRegressionTransformer(coefficients, intercept)

  override def dependentFeature = new FeatureDesc(dependentFeatureName, DoubleType())

}


object LinearRegressionModel {

  /**
   * An alternative constructor for LinearRegressionModel.
   *
   * Call this "make" instead of "apply" because the constructors need to be distinguishable after type erasure.
   *
   * @param coefficients Vector of coefficients of the model. Must match the length of inputFeatures.
   * @param inputFeatures Description of the (numeric) input features. Must match the length of coefficients.
   * @param intercept Intercept of the model (defaults to 0).
   * @param dependentFeatureName Name used to identify the dependent feature in an evaluation dataset.
   * @param identifier Used to identify this model when in a collection of models. Should be simple characters,
   *                   so it can be used in a feature name.
   */
  def make(coefficients: Seq[Double],
            inputFeatures: Seq[FeatureDesc[_ <: Number]],
            intercept: Double = 0,
            dependentFeatureName: String = "", identifier: String = ""): LinearRegressionModel = {
    LinearRegressionModel(TransformerUtil.toJavaDoubleSeq(coefficients),inputFeatures, intercept, dependentFeatureName, identifier)
  }
}
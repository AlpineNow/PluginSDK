/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
  * :: AlpineSdkApi ::
  * Object that is returned by the "get metadata" method in the plugin signature class.
  * And is used by the plugin engine to define how the operator will show up in the GUI.
  *
  * @constructor create metadata class by defining each of its fields.
  * @param name           the name of the operator as it shows up in the GUI
  * @param category       the category of operator i.e. "transformation".
  *                       Used to filter operator in the left hand panel of the workflow GUI.
  * @param author         the writer of the operator
  * @param version        the version number of this operator.
  * @param helpURL        A link to documentation about the operator
  * @param iconNamePrefix String name of the custom icon. To use the default icon,
  *                       use the empty String.
  * @param toolTipText    The text of the tool tip which appears when the user hovers over the icon
  *                       for the operator in the left hand "operators" dropdown.
  */
@AlpineSdkApi
case class OperatorMetadata(name: String,
                            category: String,
                            author: Option[String],
                            version: Int,
                            helpURL: Option[String],
                            iconNamePrefix: Option[String], toolTipText: Option[String]) {
  def this(name: String,
           category: String,
           author: String,
           version: Int,
           helpURL: String,
           iconNamePrefix: String, toolTipText: String) = {
    this(name, category, OperatorMetadata.emptyOptionIfEmptyString(author), version,
      OperatorMetadata.emptyOptionIfEmptyString(helpURL),
      OperatorMetadata.emptyOptionIfEmptyString(iconNamePrefix),
      OperatorMetadata.emptyOptionIfEmptyString(toolTipText))
  }

  def this(name: String,
           category: String,
           author: String,
           version: Int,
           helpURL: String,
           iconNamePrefix: String) = {
    this(name, category, OperatorMetadata.emptyOptionIfEmptyString(author),
      version, OperatorMetadata.emptyOptionIfEmptyString(helpURL),
      OperatorMetadata.emptyOptionIfEmptyString(iconNamePrefix), None)
  }

  def this(name: String, category: String, version: Int = 1) = {
    this(name, category, None, version, None, None, None)
  }

  def this(name: String,
           category: String,
           author: Option[String],
           version: Int,
           helpURL: Option[String],
           iconNamePrefix: Option[String]) = {
    this(name, category, author, version, helpURL, iconNamePrefix, None)
  }
}

object OperatorMetadata {

  def apply(name: String,
            category: String,
            author: String,
            version: Int,
            helpURL: String,
            iconNamePrefix: String): OperatorMetadata = {
    new OperatorMetadata(name, category, author, version, helpURL, iconNamePrefix)
  }

  def apply(name: String,
            category: String,
            author: Option[String],
            version: Int,
            helpURL: Option[String],
            iconNamePrefix: Option[String]) = {
    new OperatorMetadata(name, category, author, version, helpURL, iconNamePrefix, None)
  }

  def emptyOptionIfEmptyString(s: String): Option[String] = {
    if (s == null || s.trim.isEmpty) {
      None
    } else {
      Some(s)
    }
  }
}

object OperatorCategories {
  /**
    * Use this category for operators which return use the Alpine Model
    * Operationalization framework and which can be used with predictors.
    */
  val MODEL = "Model"

  /**
    * Use for random sample and other sample generator operators.
    */
  val SAMPLING = "Sample"

  /**
    * Use for any data cleaning or data transformation operator
    */
  val TRANSFORM = "Transform"

  /**
    * Use for graphing or data exploration operators.
    */
  val EXPLORE = "Explore"
  //0
  val LOAD_DATA = "Load" //source operators 0


  /**
    * Use for predictors and evaluator operators which
    * take the input of a model.
    */
  val PREDICTOR = "Predict" //square


  val TOOLS = "Tools" //square

  /**
    * Includes T-Tests and other operators which perform inferential statistics.
    */
  val STATISTICS = "Statistics"
  val NLP = "NLP"

}
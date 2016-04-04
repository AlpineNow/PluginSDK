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
  */
@AlpineSdkApi
case class OperatorMetadata(name: String,
                            category: String,
                            author: Option[String],
                            version: Int,
                            helpURL: Option[String],
                            iconNamePrefix: Option[String]) {
  def this(name: String,
           category: String,
           author: String,
           version: Int,
           helpURL: String,
           iconNamePrefix: String) = {
    this(name, category, OperatorMetadata.emptyOptionIfEmptyString(author), version, OperatorMetadata.emptyOptionIfEmptyString(helpURL), OperatorMetadata.emptyOptionIfEmptyString(iconNamePrefix))
  }

  def this(name: String, category: String, version: Int = 1) = {
    this(name, category, None, version, None, None)
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

  def emptyOptionIfEmptyString(s: String): Option[String] = {
    if (s == null || s.trim.isEmpty) {
      None
    } else {
      Some(s)
    }
  }
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 *  Object that is returned by the "get metadata" method in the plugin signature class.
 * And is used by the plugin engine to define how the operator will show up in the GUI.
 *
 * @constructor create metadata class by defining each of its fields.
 *             @param name the name of the operator as it shows up in the GUI
 *             @param category the category of operator i.e. "transformation".
 *                             Used to filter operator in the left hand panel of the workflow GUI.
 *             @param author the writer of the operator
 *             @param version the version number of this operator.
 *             @param helpURL A link to documentation about the operator
 *             @param iconNamePrefix String name of the custom icon. To use the default icon,
 *                                   use the empty String.
 */
@AlpineSdkApi
case class OperatorMetadata(
  val name: String,
  val category: String,
  val author: String,
  val version: Int,
  val helpURL: String,
  val iconNamePrefix: String
)

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOString, OperatorInfo}

/**
  * Abstract implementation of [[IOString]].
  * Can be extended by developers who want custom behaviour not provided by [[IOStringDefault]].
  * @param value The string value.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractIOString(val value: String,
                                val sourceOperatorInfo: Option[OperatorInfo],
                                val addendum: Map[String, AnyRef]) extends IOString {
  def displayName: String = "String"
}

/**
  * Default implementation of [[IOString]].
  * @param value The string value.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class IOStringDefault(override val value: String,
                           override val sourceOperatorInfo: Option[OperatorInfo],
                           override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIOString(value, sourceOperatorInfo, addendum) with IOString
/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOString, OperatorInfo}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend IOString.
 */
abstract class AbstractIOString(val value: String,
                                val sourceOperatorInfo: Option[OperatorInfo],
                                val addendum: Map[String, AnyRef]) extends IOString {
  def displayName: String = "String"
}

case class IOStringDefault(override val value: String,
                           override val sourceOperatorInfo: Option[OperatorInfo],
                           override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIOString(value, sourceOperatorInfo, addendum) with IOString
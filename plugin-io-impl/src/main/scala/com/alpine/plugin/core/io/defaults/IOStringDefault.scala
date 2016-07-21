/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOString, OperatorInfo}

/**
  * Abstract implementation of [[IOString]].
  * Can be extended by developers who want custom behaviour not provided by [[IOStringDefault]].
  *
  * @param value    The string value.
  * @param addendum Map containing additional information.
  */
abstract class AbstractIOString(val value: String,
                                val addendum: Map[String, AnyRef]) extends IOString

/**
  * Default implementation of [[IOString]].
  *
  * @param value    The string value.
  * @param addendum Map containing additional information.
  */
case class IOStringDefault(override val value: String,
                           override val addendum: Map[String, AnyRef])
  extends AbstractIOString(value, addendum) with IOString {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def this(value: String,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    this(value, addendum)
  }
}

object IOStringDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(value: String,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef]): IOStringDefault = {
    IOStringDefault(value, addendum)
  }

  def apply(value: String): IOStringDefault = {
    IOStringDefault(value, Map[String, AnyRef]())
  }

}

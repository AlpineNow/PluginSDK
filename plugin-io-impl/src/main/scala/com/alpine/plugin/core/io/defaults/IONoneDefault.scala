package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IONone, OperatorInfo}

/**
  * Abstract implementation of [[IONone]].
  * Can be extended by developers who want custom behaviour not provided by [[IONoneDefault]].
  *
  * @param addendum Map containing additional information.
  */
abstract class AbstractIONone(val addendum: Map[String, AnyRef]) extends IONone

/**
  * Default implementation of [[IONone]].
  *
  * @param addendum Map containing additional information.
  */
case class IONoneDefault(override val addendum: Map[String, AnyRef])
  extends AbstractIONone(addendum)

object IONoneDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(sourceOperatorInfo: Option[OperatorInfo] = None,
            addendum: Map[String, AnyRef]): IONoneDefault = {
    IONoneDefault(addendum)
  }

  def apply(): IONoneDefault = {
    new IONoneDefault(Map[String, AnyRef]())
  }
}

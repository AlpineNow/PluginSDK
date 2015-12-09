package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IONone, OperatorInfo}

/**
  * Abstract implementation of [[IONone]].
  * Can be extended by developers who want custom behaviour not provided by [[IONoneDefault]].
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractIONone(val sourceOperatorInfo: Option[OperatorInfo],
                              val addendum: Map[String, AnyRef]) extends IONone {
  def displayName: String = "None"
}

/**
  * Default implementation of [[IONone]].
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class IONoneDefault(override val sourceOperatorInfo: Option[OperatorInfo] = None,
                         override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIONone(sourceOperatorInfo, addendum)

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IONone, OperatorInfo}

abstract class AbstractIONone(val sourceOperatorInfo: Option[OperatorInfo],
                              val addendum: Map[String, AnyRef]) extends IONone {
  def displayName: String = "None"
}

case class IONoneDefault(override val sourceOperatorInfo: Option[OperatorInfo] = None,
                         override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIONone(sourceOperatorInfo, addendum)

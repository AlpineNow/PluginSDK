/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, IOList, OperatorInfo}

/**
  * Abstract implementation of [[IOList]].
  * Can be extended by developers who want custom behaviour not provided by [[IOListDefault]].
  *
  * @param elements The sub-elements that make up this list.
  * @param sources  Information about the operators that created each element, in their respective order.
  * @param addendum Map containing additional information.
  * @tparam T The type of elements in the list.
  */
abstract class AbstractIOList[T <: IOBase](val elements: Seq[T],
                                           val sources: Seq[OperatorInfo],
                                           val addendum: Map[String, AnyRef]
                                          ) extends IOList[T]

/**
  * Default implementation of [[IOList]].
  *
  * @param elements The sub-elements that make up this list.
  * @param sources  Information about the operators that created each element, in their respective order.
  * @param addendum Map containing additional information.
  * @tparam T The type of elements in the list.
  */
case class IOListDefault[T <: IOBase](override val elements: Seq[T],
                                      override val sources: Seq[OperatorInfo],
                                      override val addendum: Map[String, AnyRef])
  extends AbstractIOList(elements, sources, addendum)

object IOListDefault {

  def apply[T <: IOBase](elements: Seq[T], sources: Seq[OperatorInfo]): IOListDefault[T] = {
    IOListDefault(elements, sources, Map[String, AnyRef]())
  }
}

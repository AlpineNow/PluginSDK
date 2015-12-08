/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, IOList, OperatorInfo}

/**
  * Abstract implementation of [[IOList]].
  * Can be extended by developers who want custom behaviour not provided by [[IOListDefault]].
  * @param displayName The name used to display this object in the UI.
  * @param elements The sub-elements that make up this list.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T The type of elements in the list.
  */
abstract class AbstractIOList[T <: IOBase](val displayName: String,
                                           val elements: Seq[T],
                                           val sourceOperatorInfo: Option[OperatorInfo],
                                           val addendum: Map[String, AnyRef]
                                            ) extends IOList[T]

/**
  * Default implementation of [[IOList]].
  * @param displayName The name used to display this object in the UI.
  * @param elements The sub-elements that make up this list.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T The type of elements in the list.
  */
case class IOListDefault[T <: IOBase](override val displayName: String,
                                      override val elements: Seq[T],
                                      override val sourceOperatorInfo: Option[OperatorInfo] = None,
                                      override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIOList(displayName, elements, sourceOperatorInfo, addendum)

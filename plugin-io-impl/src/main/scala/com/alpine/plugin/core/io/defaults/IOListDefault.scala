/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, IOList, OperatorInfo}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend IOList.
 */
abstract class AbstractIOList[T <: IOBase](val displayName: String,
                                           val elements: Seq[T],
                                           val sourceOperatorInfo: Option[OperatorInfo],
                                           val addendum: Map[String, AnyRef]
                                            ) extends IOList[T]

case class IOListDefault[T <: IOBase](override val displayName: String,
                                      override val elements: Seq[T],
                                      override val sourceOperatorInfo: Option[OperatorInfo] = None,
                                      override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractIOList(displayName, elements, sourceOperatorInfo, addendum)

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, OperatorInfo, Tuple3}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend Tuple3.
 */
abstract class AbstractTuple3[
  T1 <: IOBase,
  T2 <: IOBase,
  T3 <: IOBase
](val displayName: String, val _1: T1, val _2: T2, val _3: T3,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef])
  extends Tuple3[T1, T2, T3] {
  def elements: Seq[IOBase] = Seq(_1, _2, _3)
}

case class Tuple3Default[
  T1 <: IOBase,
  T2 <: IOBase,
  T3 <: IOBase
](override val displayName: String,
  override val _1: T1,
  override val _2: T2,
  override val _3: T3,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractTuple3(displayName, _1, _2, _3, sourceOperatorInfo, addendum)

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, OperatorInfo, Tuple2}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend Tuple2.
 */
abstract class AbstractTuple2[
T1 <: IOBase,
T2 <: IOBase
](val displayName: String,
  val _1: T1,
  val _2: T2,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef])
  extends Tuple2[T1, T2] {
  def elements: Seq[IOBase] = Seq(_1, _2)
}

case class Tuple2Default[
T1 <: IOBase,
T2 <: IOBase
](override val displayName: String,
  override val _1: T1,
  override val _2: T2,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractTuple2(displayName, _1, _2, sourceOperatorInfo, addendum)

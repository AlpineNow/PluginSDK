/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, OperatorInfo, Tuple2}

/**
  * Abstract implementation of [[Tuple2]].
  * Can be extended by developers who want custom behaviour not provided by [[Tuple2Default]].
  * @param displayName The name used to display this object in the UI.
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
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

/**
  * Default implementation of [[Tuple2]]
  * @param displayName The name used to display this object in the UI.
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
  */
case class Tuple2Default[
T1 <: IOBase,
T2 <: IOBase
](override val displayName: String,
  override val _1: T1,
  override val _2: T2,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractTuple2(displayName, _1, _2, sourceOperatorInfo, addendum)

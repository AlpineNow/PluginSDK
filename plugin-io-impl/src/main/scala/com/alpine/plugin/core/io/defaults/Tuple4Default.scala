/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{Tuple3, IOBase, OperatorInfo, Tuple4}

/**
  * Abstract implementation of [[Tuple3]].
  * Can be extended by developers who want custom behaviour not provided by [[Tuple3Default]].
  * @param displayName The name used to display this object in the UI.
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param _3 The third element of the tuple.
  * @param _4 The fourth element of the tuple.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
  * @tparam T3 Type of the third element of the tuple.
  * @tparam T4 Type of the fourth element of the tuple.
  */
abstract class AbstractTuple4[
T1 <: IOBase,
T2 <: IOBase,
T3 <: IOBase,
T4 <: IOBase
](val displayName: String,
  val _1: T1,
  val _2: T2,
  val _3: T3,
  val _4: T4,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef])
  extends Tuple4[T1, T2, T3, T4] {
  def elements: Seq[IOBase] = Seq(_1, _2, _3, _4)
}

/**
  * Default implementation of [[Tuple4]].
  * @param displayName The name used to display this object in the UI.
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param _3 The third element of the tuple.
  * @param _4 The fourth element of the tuple.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
  * @tparam T3 Type of the third element of the tuple.
  * @tparam T4 Type of the fourth element of the tuple.
  */
case class Tuple4Default[
T1 <: IOBase,
T2 <: IOBase,
T3 <: IOBase,
T4 <: IOBase
](override val displayName: String,
  override val _1: T1,
  override val _2: T2,
  override val _3: T3,
  override val _4: T4,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractTuple4(displayName, _1, _2, _3, _4, sourceOperatorInfo, addendum)


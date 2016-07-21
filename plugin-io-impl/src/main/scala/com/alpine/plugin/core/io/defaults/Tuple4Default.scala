/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{Tuple3, IOBase, OperatorInfo, Tuple4}

/**
  * Abstract implementation of [[Tuple3]].
  * Can be extended by developers who want custom behaviour not provided by [[Tuple3Default]].
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param _3 The third element of the tuple.
  * @param _4 The fourth element of the tuple.
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
](val _1: T1,
  val _2: T2,
  val _3: T3,
  val _4: T4,
  val addendum: Map[String, AnyRef])
  extends Tuple4[T1, T2, T3, T4] {
  def elements: Seq[IOBase] = Seq(_1, _2, _3, _4)
}

/**
  * Default implementation of [[Tuple4]].
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param _3 The third element of the tuple.
  * @param _4 The fourth element of the tuple.
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
](override val _1: T1,
  override val _2: T2,
  override val _3: T3,
  override val _4: T4,
  override val addendum: Map[String, AnyRef])
  extends AbstractTuple4(_1, _2, _3, _4, addendum) {

  @deprecated("Use constructor without displayName and sourceOperatorInfo.")
  def this(displayName: String, _1: T1, _2: T2, _3: T3, _4: T4,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    this(_1, _2, _3, _4, addendum)
  }
}

object Tuple4Default {

  @deprecated("Use constructor without displayName and sourceOperatorInfo.")
  def apply[
  T1 <: IOBase,
  T2 <: IOBase,
  T3 <: IOBase,
  T4 <: IOBase
  ](displayName: String, _1: T1,  _2: T2, _3: T3, _4: T4,
    sourceOperatorInfo: Option[OperatorInfo],
    addendum: Map[String, AnyRef] = Map[String, AnyRef]()): Tuple4Default[T1, T2, T3, T4] = {
    Tuple4Default(_1, _2, _3, _4, addendum)
  }
  def apply[
  T1 <: IOBase,
  T2 <: IOBase,
  T3 <: IOBase,
  T4 <: IOBase
  ](_1: T1, _2: T2, _3: T3, _4: T4): Tuple4Default[T1, T2, T3, T4] = {
    Tuple4Default(_1, _2, _3, _4, Map[String, AnyRef]())
  }
}

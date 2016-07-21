/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IOBase, OperatorInfo, Tuple2}

/**
  * Abstract implementation of [[Tuple2]].
  * Can be extended by developers who want custom behaviour not provided by [[Tuple2Default]].
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
  */
abstract class AbstractTuple2[
T1 <: IOBase,
T2 <: IOBase
](val _1: T1,
  val _2: T2,
  val addendum: Map[String, AnyRef])
  extends Tuple2[T1, T2] {
  def elements: Seq[IOBase] = Seq(_1, _2)
}

/**
  * Default implementation of [[Tuple2]]
  * @param _1 The first element of the tuple.
  * @param _2 The second element of the tuple.
  * @param addendum Map containing additional information.
  * @tparam T1 Type of the first element of the tuple.
  * @tparam T2 Type of the second element of the tuple.
  */
case class Tuple2Default[
T1 <: IOBase,
T2 <: IOBase
](override val _1: T1,
  override val _2: T2,
  override val addendum: Map[String, AnyRef])
  extends AbstractTuple2(_1, _2, addendum) {

  @deprecated("Use constructor without displayName and sourceOperatorInfo.")
  def this(displayName: String, _1: T1,  _2: T2,
  sourceOperatorInfo: Option[OperatorInfo],
  addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    this(_1, _2, addendum)
  }
}

object Tuple2Default {

  @deprecated("Use constructor without displayName and sourceOperatorInfo.")
  def apply[
  T1 <: IOBase,
  T2 <: IOBase
  ](displayName: String, _1: T1,  _2: T2,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()): Tuple2Default[T1, T2] = {
    Tuple2Default(_1, _2, addendum)
  }

  def apply[
  T1 <: IOBase,
  T2 <: IOBase
  ](_1: T1,  _2: T2): Tuple2Default[T1, T2] = {
    Tuple2Default(_1, _2, Map[String, AnyRef]())
  }
}

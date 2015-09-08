/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * This is the interface for the quadruplet input/output.
 */
@AlpineSdkApi
trait Tuple4[
  T1 <: IOBase,
  T2 <: IOBase,
  T3 <: IOBase,
  T4 <: IOBase] extends Tuple {

  def _1: T1
  def _2: T2
  def _3: T3
  def _4: T4

}

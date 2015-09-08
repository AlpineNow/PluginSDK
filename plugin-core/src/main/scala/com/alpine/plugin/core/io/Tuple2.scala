/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * This is the interface for a pair input/output.
 */
@AlpineSdkApi
trait Tuple2[
  T1 <: IOBase,
  T2 <: IOBase] extends Tuple {
  def _1: T1
  def _2: T2
}

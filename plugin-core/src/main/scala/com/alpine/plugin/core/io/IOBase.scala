/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import java.io.Serializable

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * All inputs and outputs to the operators should extend this class.
 * E.g., the data set input/output classes.
 */
@AlpineSdkApi
trait IOBase extends Serializable {

  /**
    * Used to store additional information, for example information needed for visualization.
    *
    * For serialization purposes, the values in the map must be simple objects -
    * Numbers, Strings, Lists, with no custom classes.
    *
    * @return Map containing additional information.
    */
  def addendum: Map[String, AnyRef]

}

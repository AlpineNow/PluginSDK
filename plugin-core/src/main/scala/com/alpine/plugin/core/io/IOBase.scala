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
   * This is used to display a customized name of the output from the UI.
   * @return The display name for this object.
   */
  def displayName: String

  /**
   * An IOBase object is always output by an operator.
   * Off-shelf IOBase implementations use this function to return the identifying information
   * of the most recent operator that returned this object.
   * Note that if the user has changed the name of the operator since this IOBase
   * object was created, the source operator name here will be the previous name,
   * not the current one.
   *
   * Will be None if this is a Tuple or List representing IOBase objects
   * from several operators, or for IONone.
   *
   * The UUID is unique to each operator within a workflow,
   * and does not change for the lifetime of the operator.
   *
   * @return The name of the source operator that returned this.
   */
  def sourceOperatorInfo: Option[OperatorInfo]

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

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.visualization

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait VisualModel

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait CompositeVisualModel extends VisualModel {
  /**
   * Add a child visual model. The child may not be another composite.
   * @param name The name of the child visual model.
   * @param visualModel The visual model we want to add as a child.
   */
  def addVisualModel(name: String, visualModel: VisualModel): Unit
}
/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

import scala.collection.mutable.ListBuffer

/**
  * A visual model composed of other visual models.
  * Each sub-model will be displayed in a separate tab.
  * Sub-models can also be composite models.
  */
class CompositeVisualModel extends VisualModel {

  private val modelList = ListBuffer[(String, VisualModel)]()

  /**
    * Adds a sub-model to the composite model.
    *
    * @param name     Name of the added model. This will be the name of the tab it is displayed in.
    * @param subModel The model to add to the composite model.
    */
  def addVisualModel(name: String, subModel: VisualModel): Unit = {
    modelList.append((name, subModel))
  }

  /**
    * Returns a list of all sub-models that have been added to this model, with their names.
    *
    * @return List of (name, sub-model) pairs.
    */
  def subModels: Seq[(String, VisualModel)] = modelList
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.visualization

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.{DBTable, IOBase, TabularDataset}

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait VisualModelFactory {
  /**
   * One can return a composite of multiple visualizations.
   * @return A composite visual model that can contain multiple visualizations.
   */
  def createCompositeVisualModel(): CompositeVisualModel

  /**
   * Create the default visual model for the given IOBase object.
   * This is useful to quickly generate a visual model for an existing IOBase
   * object.
   * @param ioObject The IOBase object that we want to create a visual model for.
   * @return A matching visual model.
   */
  def createDefaultVisualModel(ioObject: IOBase): VisualModel

  /**
   * Create a simple text content visualization.
   * @param text The text we want to display in the console.
   * @return A text visualization object.
   */
  def createTextVisualization(text: String): VisualModel

  /**
   * Create a visualization for an Hdfs tabular dataset.
   * @param dataset A Hdfs tabular dataset that we want to visualize.
   * @return A visualization of the sample.
   */
  def createTabularDatasetVisualization(dataset: TabularDataset): VisualModel

  /**
   * Create a visualization for a DB table.
   * @param dbtable A DB table that we want to visualize.
   * @return A visualization of the sample.
   */
  def createDBTableVisualization(dbtable: DBTable): VisualModel

  /**
   * Create a visualization of an HTML text element.
   * Use this method rather than the 'createTextVisualization' method if you want to include
   * HTML formatting elements (like a table, or bold text) in your visualization.
   * @param text An HTML String
   * @return A text visualization object, which puts the html text inside of a <text> tag.
   */
  def createHtmlTextVisualization(text : String ) : VisualModel

}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.visualization

import java.util.Locale

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.{DBTable, HdfsFile, IOBase, TabularDataset}

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
trait VisualModelFactory {
  /**
   * One can return a composite of multiple visualizations.
   * @return A composite visual model that can contain multiple visualizations.
   */
  @deprecated("Use CompositeVisualModel directly instead.")
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
  @deprecated("Use TextVisualModel directly instead.")
  def createTextVisualization(text: String): VisualModel

  /**
   * Create a visualization for an Hdfs tabular dataset.
   * @param dataset A Hdfs tabular dataset that we want to visualize.
   * @return A visualization of the sample.
   */
  def createTabularDatasetVisualization(dataset: TabularDataset): VisualModel

  /**
    * Gets the first few lines of the HdfsFile as plain text.
    * @param hdfsFile The file to preview.
    * @return The first few lines of the file as a visual model.
    */
  def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel

  /**
   * Create a visualization for a DB table.
    * This pulls a preview of the DB table, for display in the results console.
    * Equivalent to [[createDBTableVisualization(dbTable.schemaName, dbTable.tableName)]]
    *
   * @param dbTable A DB table that we want to visualize.
   * @return A visualization of the sample.
   */
  def createDBTableVisualization(dbTable: DBTable): VisualModel

  /**
    * Create a visualization for a DB table.
    * This pulls a preview of the DB table, for display in the results console.
    *
    * @param schemaName The schema name of the table to preview.
    * @param tableName  The schema name of the table to preview.
    * @return A visualization of a sample of the table.
    */
  def createDBTableVisualization(schemaName: String, tableName: String): VisualModel

  /**
   * Create a visualization of an HTML text element.
   * Use this method rather than the 'createTextVisualization' method if you want to include
   * HTML formatting elements (like a table, or bold text) in your visualization.
   * @param text An HTML String
   * @return A text visualization object, which puts the html text inside of a <text> tag.
   */
  @deprecated("Use HtmlVisualModel directly instead.")
  def createHtmlTextVisualization(text: String): VisualModel

  def locale: Locale

}

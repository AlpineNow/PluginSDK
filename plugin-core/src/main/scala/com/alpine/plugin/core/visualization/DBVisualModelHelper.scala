/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

import java.util.Locale

import com.alpine.plugin.core.io.DBTable

/**
  * Created by Jennifer Thompson on 2/16/17.
  */
trait DBVisualModelHelper {

  // TODO: Return type for these methods should be TabularVisualModel.

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

  def locale: Locale

}

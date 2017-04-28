/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.test.mock

import java.util.Locale

import com.alpine.plugin.core.io.DBTable
import com.alpine.plugin.core.visualization.{DBVisualModelHelper, TextVisualModel, VisualModel}

/**
  * Created by Jennifer Thompson on 3/24/17.
  */
class DBVisualModelHelperMock extends DBVisualModelHelper {

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    createDBTableVisualization(dbTable.schemaName, dbTable.tableName)
  }

  override def createDBTableVisualization(schemaName: String, tableName: String): VisualModel = {
    TextVisualModel("In the production environment, this would be replaced with a preview of the contents of the table at "
      + schemaName + "." + tableName + " from the database.")
  }

  override def locale: Locale = Locale.ENGLISH
}

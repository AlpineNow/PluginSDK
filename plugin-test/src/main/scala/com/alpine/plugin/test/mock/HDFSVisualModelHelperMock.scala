/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.test.mock

import java.util.Locale

import com.alpine.plugin.core.io.{HdfsFile, TabularDataset}
import com.alpine.plugin.core.visualization.{HDFSVisualModelHelper, TabularVisualModel, TextVisualModel}

/**
  * Created by Jennifer Thompson on 3/24/17.
  */
class HDFSVisualModelHelperMock extends HDFSVisualModelHelper {

  override def createTabularDatasetVisualization(dataset: TabularDataset): TabularVisualModel = {
    TabularVisualModel(Seq(), dataset.tabularSchema.definedColumns)
  }

  override def createTabularDatasetVisualization(dataset: TabularDataset, rowsToFetch: Int): TabularVisualModel = {
    TabularVisualModel(Seq(), dataset.tabularSchema.definedColumns)
  }

  override def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel = {
    TextVisualModel("In the production environment, this would be replaced with a preview of the contents of the file at "
      + hdfsFile.path + " on the hadoop cluster.")
  }

  override def locale: Locale = Locale.ENGLISH
}

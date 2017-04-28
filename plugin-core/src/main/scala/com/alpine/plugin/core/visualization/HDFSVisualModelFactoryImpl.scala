package com.alpine.plugin.core.visualization

import java.util.Locale

import com.alpine.plugin.core.io._

/**
  * Created by Jennifer Thompson on 2/16/17.
  */
class HDFSVisualModelFactoryImpl(var hdfsVisualHelper: HDFSVisualModelHelper) extends VisualModelFactory {

  override def createCompositeVisualModel() = new CompositeVisualModel

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    ioObject match {
      case dataset: TabularDataset => createTabularDatasetVisualization(dataset)
      case string: IOString => TextVisualModel(string.value)
      case file: HdfsFile =>
        // We have this condition after the TabularDataset one,
        // because some TabularDatasets are HdfsFiles.
        createPlainTextVisualModel(file)
      case _ => TextVisualModel(
        "Alpine currently doesn't have a visualization module registered for " + ioObject.getClass.getCanonicalName
      )
    }
  }

  override def createTextVisualization(text: String): TextVisualModel = {
    TextVisualModel(text)
  }

  override def createTabularDatasetVisualization(tabularDataset: TabularDataset): TabularVisualModel = {
    hdfsVisualHelper.createTabularDatasetVisualization(tabularDataset)
  }

  override def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel = {
    hdfsVisualHelper.createPlainTextVisualModel(hdfsFile)
  }

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    throw new RuntimeException("Cannot generate DB Table visualization in a Spark operator.")
  }

  override def createDBTableVisualization(schemaName: String, tableName: String): VisualModel = {
    throw new RuntimeException("Cannot generate DB Table visualization in a Spark operator.")
  }

  override def createHtmlTextVisualization(text: String): HtmlVisualModel = {
    HtmlVisualModel(text)
  }

  override def locale: Locale = {
    hdfsVisualHelper.locale
  }
}
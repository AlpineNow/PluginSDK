package com.alpine.plugin.core.visualization

import java.util.Locale

import com.alpine.plugin.core.io._

/**
  * Created by Jennifer Thompson on 2/16/17.
  */
class DBVisualModelFactoryImpl(var dbVisualHelper: DBVisualModelHelper) extends VisualModelFactory {

  override def createCompositeVisualModel() = new CompositeVisualModel

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    ioObject match {
      case dbTable: DBTable => createDBTableVisualization(dbTable)
      case string: IOString => TextVisualModel(string.value)
      case _ => TextVisualModel(
        "Alpine currently doesn't have a visualization module registered for " + ioObject.getClass.getCanonicalName
      )
    }
  }

  override def createTextVisualization(text: String): TextVisualModel = {
    TextVisualModel(text)
  }

  override def createTabularDatasetVisualization(tabularDataset: TabularDataset): VisualModel = {
    throw new RuntimeException("Cannot generate HDFS visualization in a DB operator.")
  }

  override def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel = {
    throw new RuntimeException("Cannot generate HDFS visualization in a DB operator.")
  }

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    dbVisualHelper.createDBTableVisualization(dbTable)
  }

  override def createDBTableVisualization(schemaName: String, tableName: String): VisualModel = {
    dbVisualHelper.createDBTableVisualization(schemaName, tableName)
  }

  override def createHtmlTextVisualization(text: String): HtmlVisualModel = {
    HtmlVisualModel(text)
  }

  override def locale: Locale = dbVisualHelper.locale
}
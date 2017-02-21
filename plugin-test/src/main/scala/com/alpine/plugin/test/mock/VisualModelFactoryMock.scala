package com.alpine.plugin.test.mock

import java.util.Locale

import com.alpine.plugin.core.io._
import com.alpine.plugin.core.visualization._

class VisualModelFactoryMock extends VisualModelFactory {

  override def createCompositeVisualModel(): CompositeVisualModel = {
    new CompositeVisualModel
  }

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    ioObject match {
      case (t: TabularDataset) => createTabularDatasetVisualization(t)
      case (d: DBTable) => createDBTableVisualization(d)
      case (s: IOString) => TextVisualModel(s.value)
      case (h: HdfsFile) => createPlainTextVisualModel(h)
      case (_) => TextVisualModel(
        "Alpine currently doesn't have a visualization module registered for " +
          ioObject.getClass.getCanonicalName
      )
    }
  }

  override def createTextVisualization(text: String): TextVisualModel = {
    TextVisualModel(text)
  }

  override def createTabularDatasetVisualization(dataset: TabularDataset): TabularVisualModel = {
    TabularVisualModel(Seq(), dataset.tabularSchema.definedColumns)
  }

  override def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel = {
    TextVisualModel("In the production environment, this would be replaced with a preview of the contents of the file at "
      + hdfsFile.path + " on the hadoop cluster.")
  }

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    createDBTableVisualization(dbTable.schemaName, dbTable.tableName)
  }

  override def createDBTableVisualization(schemaName: String, tableName: String): VisualModel = {
    TextVisualModel("In the production environment, this would be replaced with a preview of the contents of the table at "
      + schemaName + "." + tableName + " from the database.")
  }

  override def createHtmlTextVisualization(text: String): HtmlVisualModel = {
    HtmlVisualModel(text)
  }

  override def locale: Locale = Locale.ENGLISH

}

package com.alpine.plugin.test.mock

import java.util.Locale

import com.alpine.plugin.core.io.{DBTable, IOBase, IOString, TabularDataset}
import com.alpine.plugin.core.visualization._

class VisualModelFactoryMock extends VisualModelFactory {

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    DBTableVisualModelMock(dbTable)
  }

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    ioObject match {
      case (t: TabularDataset) => createTabularDatasetVisualization(t)
      case (d: DBTable) => createDBTableVisualization(d)
      case (s: IOString) => TextVisualModel(s.value)
      case (_) => TextVisualModel(
        "Alpine currently doesn't have a visualization module registered for " +
          ioObject.getClass.getCanonicalName
      )
    }
  }

  override def createHtmlTextVisualization(text: String): HtmlVisualModel = {
    HtmlVisualModel(text)
  }

  override def createCompositeVisualModel(): CompositeVisualModel = {
    new CompositeVisualModel
  }

  override def createTabularDatasetVisualization(dataset: TabularDataset): TabularVisualModel = {
    TabularVisualModel(Seq(), dataset.tabularSchema.definedColumns)
  }

  override def createTextVisualization(text: String): TextVisualModel = {
    TextVisualModel(text)
  }

  override def locale: Locale = Locale.ENGLISH
}

case class DBTableVisualModelMock(dbTable: DBTable) extends VisualModel {}

package com.alpine.plugin.test.mock

import com.alpine.plugin.core.io.{DBTable, IOBase, TabularDataset}
import com.alpine.plugin.core.visualization._

class VisualModelFactoryMock extends VisualModelFactory {

  override def createDBTableVisualization(dbTable: DBTable): VisualModel = {
    new DBTableVisualModelMock(dbTable)
  }

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    ioObject match {
      case (t: TabularDataset) => createTabularDatasetVisualization(t)
      case (_) => createTextVisualization(ioObject.displayName)
    }
  }

  override def createHtmlTextVisualization(text: String): HtmlVisualModel = {
    new HtmlVisualModel(text)
  }

  override def createCompositeVisualModel(): CompositeVisualModel = {
    new CompositeVisualModel
  }

  override def createTabularDatasetVisualization(dataset: TabularDataset): TabularVisualModel = {
    new TabularVisualModel(Seq(), dataset.tabularSchema.definedColumns)
  }

  override def createTextVisualization(text: String): TextVisualModel = {
    new TextVisualModel(text)
  }

}

case class DBTableVisualModelMock(dbTable: DBTable) extends VisualModel {}

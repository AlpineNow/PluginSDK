package com.alpine.plugin.test.mock

import com.alpine.plugin.core.io.{DBTable, IOBase, TabularDataset}
import com.alpine.plugin.core.visualization.{CompositeVisualModel, VisualModel, VisualModelFactory}


class VisualModelFactoryMock extends VisualModelFactory {
  private val allModels = scala.collection.mutable.ListBuffer[VisualModel]()

  def getAllModels = allModels.toList

  override def createDBTableVisualization(dbtable: DBTable): VisualModel = {
    val t = new TabularDatasetVisualModel(dbtable.asInstanceOf[TabularDataset])
    allModels.append(t)
    t
  }

  override def createDefaultVisualModel(ioObject: IOBase): VisualModel = {
    val m = ioObject match {
      case (t: TabularDataset) => createTabularDatasetVisualization(t)
      case (_) => createTextVisualization(ioObject.displayName)
    }
    allModels.append(m)
    m
  }

  override def createHtmlTextVisualization(text: String): VisualModel = {
    val m = new HtmlVisualModel(text)
    allModels.append(m)
    m
  }

  override def createCompositeVisualModel(): CompositeVisualModel = {
    val m = new CompositeVisualModelImpl
    allModels.append(m)
    m
  }

  override def createTabularDatasetVisualization(dataset: TabularDataset):
  VisualModel = {
    val m = new TabularDatasetVisualModel(dataset)
    allModels.append(m)
    m
  }

  override def createTextVisualization(text: String): VisualModel = {
    val m = new TextVisualModel(text)
    allModels.append(m)
    m
  }

}

case class TextVisualModel(text: String) extends VisualModel {}

case class HtmlVisualModel(text: String) extends VisualModel {
  override def toString = text.replace("<tab>", " ").replace("<br>", "\n")
}

case class TabularDatasetVisualModel(dataset: TabularDataset) extends VisualModel {}


case class CompositeVisualModelImpl() extends CompositeVisualModel {
  private val modelList = scala.collection.mutable.ListBuffer[(String, VisualModel)]()

  override def addVisualModel(name: String, visualModel: VisualModel): Unit = {
    modelList.append((name, visualModel))
  }

  lazy val models = modelList.toArray

}

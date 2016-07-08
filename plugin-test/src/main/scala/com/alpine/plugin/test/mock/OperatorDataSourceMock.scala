package com.alpine.plugin.test.mock

import com.alpine.plugin.core.datasource.{DataSource, OperatorDataSourceManager}
import com.alpine.plugin.core.io.{OperatorSchemaManager, TabularSchema}


class OperatorDataSourceManagerMock(source: DataSourceMock,
                                    allSource: List[DataSourceMock] = List[DataSourceMock]())
  extends OperatorDataSourceManager {

  private var runtimeSource: DataSource = source

  override def getRuntimeDataSource(): DataSource = runtimeSource

  override def setRuntimeDataSource(dataSource: DataSource): Unit = {
    runtimeSource = dataSource
  }

  override def getDataSources: Iterator[DataSource] = (source :: allSource).toIterator

  override def getDataSource(name: String): DataSource =
    if (name == source.getName)
      source
    else {
      allSource.find(_.getName.equals(name)).get
    }
}

object OperatorDataSourceManagerMock {
  def apply(dataSourceName: String) = new OperatorDataSourceManagerMock(new DataSourceMock(dataSourceName))
}


class OperatorSchemaManagerMock() extends OperatorSchemaManager {

  private var outputSchemaOption: Option[TabularSchema] = None

  override def getOutputSchema(): TabularSchema = outputSchemaOption.get

  override def setOutputSchema(outputSchema: TabularSchema): Unit = {
    outputSchemaOption = Some(outputSchema)
  }
}

case class DataSourceMock(override val getName: String) extends DataSource

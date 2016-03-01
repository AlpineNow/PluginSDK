package com.alpine.plugin.test.mock

import com.alpine.plugin.core.datasource.{DataSource, OperatorDataSourceManager}
import com.alpine.plugin.core.io.{HdfsTabularDataset, OperatorSchemaManager, TabularSchema}


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


class OperatorSchemaManagerMock(startingInputSchemaOption: Option[TabularSchema]) extends OperatorSchemaManager {

  private var inputSchemaOption = startingInputSchemaOption

  private var outputSchemaOption: Option[TabularSchema] = None

  override def getInputSchema(): TabularSchema = inputSchemaOption.get

  override def setInputSchema(inputSchema: TabularSchema): Unit = {
    inputSchemaOption = Some(inputSchema)
  }

  override def getOutputSchema(): TabularSchema = outputSchemaOption.get

  override def setOutputSchema(outputSchema: TabularSchema): Unit = {
    outputSchemaOption = Some(outputSchema)
  }
}

object OperatorSchemaManagerMock {
  def apply(input: HdfsTabularDataset) = new OperatorSchemaManagerMock(Some(input.tabularSchema))

  def apply(schema: TabularSchema) = new OperatorSchemaManagerMock(Some(schema))

  def apply(schemaOption: Option[TabularSchema]) = new OperatorSchemaManagerMock(schemaOption)
}

case class DataSourceMock(override val getName: String) extends DataSource

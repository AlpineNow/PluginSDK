/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.datasource

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
  * :: AlpineSdkApi ::
  * Used to keep track of DataSources associated with this work flow.
  * This is used in GUI to fetch a plugins inputs.
  */
@AlpineSdkApi
trait OperatorDataSourceManager {
  /**
    * Returns an Iterator of all the DataSource
    */
  @deprecated("Use getAvailableDataSources")
  def getDataSources: Iterator[DataSource]

  /**
    * The available data sources for the operator.
    * Will be filtered by hadoop / db sources depending on the Custom operator Runtime class.
    *
    * @return data sources that the operator could be run on.
    */
  def getAvailableDataSources: Seq[DataSource]

  /**
    * Returns a DataSource given its name as a string.
    */
  def getDataSource(name: String): DataSource

  /**
    * The DataSource used at runtime (there can only be one)
    */
  def getRuntimeDataSource: DataSource

  /**
    * Set the DataSource that will be used at runtime (there can only be one)
    *
    * @param dataSource The DataSource to use at runtime
    */
  def setRuntimeDataSource(dataSource: DataSource): Unit
}

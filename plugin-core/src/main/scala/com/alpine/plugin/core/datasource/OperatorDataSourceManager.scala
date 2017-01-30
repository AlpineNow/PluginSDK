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
  def getDataSources: Iterator[DataSource]

  /**
    * Returns a DataSource given its name as a string.
    */
  def getDataSource(name: String): DataSource

  /**
    * The DataSource used at runtime (there can only be one)
    */
  def getRuntimeDataSource(): DataSource

  /**
    * Set the DataSource that will be used at runtime (there can only be one)
    *
    * @param dataSource
    */
  def setRuntimeDataSource(dataSource: DataSource): Unit
}

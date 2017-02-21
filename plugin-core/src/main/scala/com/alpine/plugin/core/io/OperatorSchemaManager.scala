/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io

import com.alpine.plugin.{EmptyIOMetadata, TabularIOMetadataDefault}
import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.datasource.DataSource

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
trait OperatorSchemaManager {
  def getOutputSchema: TabularSchema

  def setOutputSchema(outputSchema: TabularSchema): Unit
}

class SimpleOperatorSchemaManager extends OperatorSchemaManager {
  var outputSchema: TabularSchema = TabularSchema(Seq(), isPartial = false)

  override def getOutputSchema: TabularSchema = outputSchema

  override def setOutputSchema(outputSchema: TabularSchema): Unit = {
    this.outputSchema = outputSchema
  }

  def toOutputMetadata(dataSource: DataSource): IOMetadata = {
    this.outputSchema match {
      case TabularSchema(Seq(), false) => EmptyIOMetadata()
      case nonEmpty => TabularIOMetadataDefault(nonEmpty, dataSource)
    }
  }
}

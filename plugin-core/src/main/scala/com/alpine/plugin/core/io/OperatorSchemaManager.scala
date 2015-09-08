/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
trait OperatorSchemaManager {
  def getOutputSchema(): TabularSchema
  def setOutputSchema(outputSchema: TabularSchema): Unit
  def getInputSchema(): TabularSchema
  def setInputSchema(inputSchema: TabularSchema): Unit
}

/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin

import com.alpine.plugin.core.OperatorParameters
import com.alpine.plugin.core.datasource.{DataSource, OperatorDataSourceManager}
import com.alpine.plugin.core.io.{IOMetadata, OperatorInfo, TabularSchema}

/**
  * Contains information that the operator may need in OperatorGUINode#getOperatorStatus.
  * We use a single class for all the arguments to make it easier to add things without breaking
  * compatibility with old operators.
  *
  * @param inputMetadata             The metadata for the operators that are connected to this one.
  *                                  This is metadata about the IOBase object that will be received from each parent operator at runtim.
  * @param parameters                The current parameter values of the operator.
  * @param operatorDataSourceManager This contains that available data-sources
  *                                  (filtered for hadoop or database depending on the operator runtime class)
  *                                  that could be used by the operator at runtime.
  */
class OperatorDesignContext(val inputMetadata: Map[OperatorInfo, IOMetadata],
                            val parameters: OperatorParameters,
                            val operatorDataSourceManager: OperatorDataSourceManager
                           ) {
  /**
    * Equivalent to the input schemas argument that used to be passed directly to the
    * onInputOrParameterChange of OperatorGUINode by the Alpine Engine.
    */
  lazy val inputSchemas: Map[String, TabularSchema] = inputMetadata
    .flatMap { case (opInfo, metadata) =>
      metadata match {
        case tabular: TabularIOMetadata => Some(opInfo.uuid, tabular.tabularSchema)
        case _ => None
      }
    }
}

/**
  * Used when the operator has no information to pass along.
  */
case class EmptyIOMetadata() extends IOMetadata

/**
  * Used to pass along column and data source information when the data-source
  * is expected to be tabular.
  */
trait TabularIOMetadata extends IOMetadata {
  def tabularSchema: TabularSchema

  def datasource: DataSource
}

/**
  * The default class for pass along TabularSchema and data-source information.
  *
  * Note that if the TabularSchema used here has the isPartial flag to set to true,
  * then we may wrap this in AugmentedTabularMetadata, if we can get the full TabularSchema
  * from step run results.
  */
case class TabularIOMetadataDefault(tabularSchema: TabularSchema,
                                    datasource: DataSource) extends TabularIOMetadata

/**
  * Used when the original metadata produced by the operator had a partial schema.
  * We then check the step run results, if they exist and match the current operator
  * parameters, and pass along that tabularSchema.
  *
  * @param tabularSchema    TabularSchema from step run results.
  * @param originalMetadata Original Metadata, produced by the OperatorGUINode.
  */
case class AugmentedTabularMetadata(tabularSchema: TabularSchema,
                                    originalMetadata: TabularIOMetadata) extends TabularIOMetadata {
  override def datasource: DataSource = originalMetadata.datasource
}

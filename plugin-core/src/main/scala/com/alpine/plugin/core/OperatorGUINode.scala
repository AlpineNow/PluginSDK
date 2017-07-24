/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core

import com.alpine.plugin._
import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}

/**
  * :: AlpineSdkApi ::
  * Control the behavior of the operator GUI node within the editor.
  */
@AlpineSdkApi
abstract class OperatorGUINode[I <: IOBase, O <: IOBase] {
  /**
    * Define actions to be performed when the operator GUI node is placed in
    * the workflow. This involves defining the property dialog content and/or
    * refining what the output schema is supposed to be like. E.g., if the output
    * is a tabular dataset, provide some outline about the output schema (partial
    * or complete).
    *
    * @param operatorDialog            The operator dialog where the operator could add
    *                                  input text boxes, etc. to define UI for parameter
    *                                  inputs.
    * @param operatorDataSourceManager This contains that available data-sources
    *                                  (filtered for hadoop or database depending on the operator runtime class)
    *                                  that could be used by the operator at runtime.
    * @param operatorSchemaManager     This can be used to provide information about
    *                                  the nature of the output/input schemas.
    *                              E.g., provide the output schema.
    */
  def onPlacement(
    operatorDialog: OperatorDialog,
    operatorDataSourceManager: OperatorDataSourceManager,
    operatorSchemaManager: OperatorSchemaManager
  ): Unit

  /**
    * Since Alpine 6.3, SDK 1.9.
    *
    * This is called to get the current status of the operator,
    * i.e. whether it is valid, information about the expected runtime output,
    * and error messages to display in the properties window.
    *
    * This is intended to replace onInputOrParameterChange, as we want to be able
    * to pass more general metadata between operators instead of only TabularSchema.
    *
    * The default implementation calls onInputOrParameterChange, to maintain compatibility
    * with old operators.
    *
    * @param context contains information about the input operators, the current parameters, and the available data-sources.
    * @return the current status of the operator.
    */
  def getOperatorStatus(context: OperatorDesignContext): OperatorStatus = {
    val operatorSchemaManager = new SimpleOperatorSchemaManager
    val operatorStatus = onInputOrParameterChange(context.inputSchemas, context.parameters, operatorSchemaManager)
    operatorStatus match {
      case OperatorStatus(isValid, msg, EmptyIOMetadata()) =>
        val outputMetadataFromSchemaManager = operatorSchemaManager
          .toOutputMetadata(context.operatorDataSourceManager.getRuntimeDataSource)
        OperatorStatus(isValid, msg, outputMetadataFromSchemaManager)
      case _ =>
        operatorStatus
    }
  }

  /**
    * This will be called by the default implementation of getOperatorStatus.
    *
    * As of Alpine 6.3, SDK 1.9, this is no longer called by the Alpine Engine.
    *
    * @param inputSchemas          If the connected inputs contain tabular schemas, this is
    *                              where they can be accessed, each with unique Ids.
    * @param params                The current parameter values of the operator.
    * @param operatorSchemaManager This should be used to change the input/output
    *                              schema, etc.
    * @return A status object about whether the inputs and/or parameters are valid.
    *         The default implementation assumes that the connected inputs and/or
    *         parameters are valid.
    */
  protected def onInputOrParameterChange(
    inputSchemas: Map[String, TabularSchema],
    params: OperatorParameters,
    operatorSchemaManager: OperatorSchemaManager
  ): OperatorStatus = {
    OperatorStatus(isValid = true, msg = None, EmptyIOMetadata())
  }

  /**
    * This is kept only for old operators. New ones should implement
    * OperatorRuntime#createVisualResults, which has more things available.
    * If neither are implemented, Alpine will generate default a visualization.
    *
    * This is invoked for GUI to customize the operator output visualization after
    * the operator finishes running. Each output should have associated default
    * visualization, but the developer can customize it here.
    *
    * @param params        The parameter values to the operator.
    * @param output        This is the output from running the operator.
    * @param visualFactory For creating visual models.
    * @return The visual model to be sent to the GUI for visualization.
    */
  def onOutputVisualization(
    params: OperatorParameters,
    output: O,
    visualFactory: VisualModelFactory
  ): VisualModel = {
    throw new NotImplementedError()
  }
}

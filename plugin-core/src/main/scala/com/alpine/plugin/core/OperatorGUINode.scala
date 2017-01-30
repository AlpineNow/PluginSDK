/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.datasource.OperatorDataSourceManager
import com.alpine.plugin.core.dialog.OperatorDialog
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.visualization.{VisualModel, VisualModelFactory}

/**
  * This is the required return type for the onInputOrParameterChange function
  * in OperatorGUINode class. If the schemas of connected inputs or selected
  * parameters are invalid, this should be false, along with an optional message
  * about why this is false.
  *
  * @param isValid true if the operator is valid. false otherwise.
  * @param msg     An optional message that will show up in the UI. You can return a
  *                message even if the operator is valid.
  */
case class OperatorStatus(
                           isValid: Boolean,
                           msg: Option[String]
                         )

object OperatorStatus {
  def apply(isValid: Boolean): OperatorStatus = {
    OperatorStatus(isValid = isValid, msg = None)
  }

  def apply(isValid: Boolean, msg: String): OperatorStatus = {
    OperatorStatus(isValid = isValid, msg = Some(msg))
  }
}

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
    * @param operatorDataSourceManager Before executing the runtime of the operator
    *                                  the developer should determine the underlying
    *                                  platform that the runtime will execute against.
    *                                  E.g., it is possible for an operator to have
    *                                  accesses to two different Hadoop clusters
    *                                  or multiple databases. A runtime can run
    *                                  on only one platform. A default platform
    *                                  will be used if nothing is done.
    * @param operatorSchemaManager     This can be used to provide information about
    *                                  the nature of the output/input schemas.
    *                              E.g., provide the output schema.
    */
  def onPlacement(
                   operatorDialog: OperatorDialog,
                   operatorDataSourceManager: OperatorDataSourceManager,
                   operatorSchemaManager: OperatorSchemaManager): Unit

  /**
    * If there's a change in the inputs/connections or parameters then this
    * function will get called so that the operator can redefine the input/output
    * schema.
    *
    * @param inputSchemas          If the connected inputs contain tabular schemas, this is
    *                              where they can be accessed, each with unique Ids.
    * @param params                The current parameter values to the operator.
    * @param operatorSchemaManager This should be used to change the input/output
    *                              schema, etc.
    * @return A status object about whether the inputs and/or parameters are valid.
    *         The default implementation assumes that the connected inputs and/or
    *         parameters are valid.
    */
  def onInputOrParameterChange(
                                inputSchemas: Map[String, TabularSchema],
                                params: OperatorParameters,
                                operatorSchemaManager: OperatorSchemaManager): OperatorStatus = {
    OperatorStatus(isValid = true, msg = None)
  }

  /**
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
                             visualFactory: VisualModelFactory): VisualModel = {
    visualFactory.createDefaultVisualModel(output)
  }
}

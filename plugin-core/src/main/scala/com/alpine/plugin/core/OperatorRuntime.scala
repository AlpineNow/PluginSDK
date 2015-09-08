/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.IOBase

/**
 * :: AlpineSdkApi ::
 * A separate instance of operator plugin runtime gets instantiated for each
 * 'run' of the workflow (or a 'step-run'). When the run is finished, the
 * instance will get deleted/garbage-collected without reuses for subsequent
 * runs.
 */
@AlpineSdkApi
abstract class OperatorRuntime[
  CTX <: ExecutionContext,
  I <: IOBase,
  O <: IOBase] {

  /**
   * This is the function that gets called when the workflow is run and the
   * operator starts running.
   * @param context Execution context of the operator.
   * @param input The input to the operator.
   * @param params The parameter values to the operator.
   * @param listener The listener object to communicate information back to
   *                 the console.
   * @return The output from the execution.
   */
  def onExecution(
    context: CTX,
    input: I,
    params: OperatorParameters,
    listener: OperatorListener
  ): O

  /**
   * This is called when the user clicks on 'stop'. If the operator is
   * currently running, this function gets called while 'onExecution' is still
   * running. So it's the developer's responsibility to properly stop whatever
   * is going within 'onExecution'.
   * @param context Execution context of the operator.
   * @param listener The listener object to communicate information back to
   *                 the console.
   */
  def onStop(
    context: CTX,
    listener: OperatorListener
  ): Unit
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.db

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.IOBase
import com.alpine.plugin.core.{OperatorListener, OperatorRuntime}

/**
 * :: AlpineSdkApi ::
 * Defines a database operators runtime behavior.
 * Override the 'onExecution' method to define the the database transformations you want.
 * Use the 'databaseExecution context' to execute SQL queries.
 * @tparam I the input type of the plugin (Must correspond to the input type of the GUI node
 *           and plugin signature)
 * @tparam O the output type of the plugin
 */
@AlpineSdkApi
abstract class DBRuntime[I <: IOBase, O <: IOBase]
  extends OperatorRuntime[DBExecutionContext, I, O] {

  /**
   * The default implementation does nothing.
   *
   * If there are processes to be stopped when the operator is stopped,
   * then the operator should override this method.
   * @param context Execution context of the operator.
   * @param listener The listener object to communicate information back to
   *                 the console.
   */
  override def onStop(context: DBExecutionContext,
                      listener: OperatorListener): Unit = {}
}

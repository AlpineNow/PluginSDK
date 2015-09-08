/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
abstract class OperatorSignature[G <: OperatorGUINode[_, _], R <: OperatorRuntime[_, _, _]] {
  /**
   * This should be implemented by every operator to provide metadata
   * about the operator itself.
   * @return Metadata about this operator. E.g. the version, the author, the
   *         category, etc.
   */
  def getMetadata(): OperatorMetadata
}

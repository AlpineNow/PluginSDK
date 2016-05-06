/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.IOBase
import com.alpine.plugin.generics.GenericUtils
import java.lang.reflect.{ParameterizedType, Type}

/** :: AlpineSdkApi ::
  * Bundles operator types and metadata used to host the operator
  * in the running system. Descendants must have a no-arguments
  * constructor
  *
  */
@AlpineSdkApi
abstract class OperatorSignature[
  G <: OperatorGUINode[_ <: IOBase, _ <: IOBase],
  R <: OperatorRuntime[_ <: ExecutionContext, _ <: IOBase, _ <: IOBase]
] {

  /**
   * This must be implemented by every operator to provide metadata
   * about the operator itself.
   * @return Metadata about this operator. E.g. the version, the author, the
   *         category, etc.
   */
  def getMetadata: OperatorMetadata

  private def toClass(in: Type): Class[_] =
    in match {
      case value: ParameterizedType => value.getRawType.asInstanceOf[Class[_]]
      case value: Class[_] => value
      case _ => throw new AssertionError("not prepared for " + in.getClass)
    }

  private val genericArgumentMap =
    GenericUtils.getAncestorClassGenericTypeArguments(getClass, classOf[OperatorSignature[_,_]].getName).get

  private[plugin] val guiNodeClass = toClass(genericArgumentMap("G")).asInstanceOf[Class[G]]

  private val guiNodeClassArgumentMap =
    GenericUtils.getAncestorClassGenericTypeArguments(guiNodeClass, classOf[OperatorGUINode[_,_]].getName).get

  private[plugin] val guiNodeInputClass = guiNodeClassArgumentMap("I")
  private[plugin] val guiNodeOutputClass = guiNodeClassArgumentMap("O")

  private[plugin] val runtimeClass = toClass(genericArgumentMap("R")).asInstanceOf[Class[R]]

  private val runtimeClassArgumentMap =
    GenericUtils.getAncestorClassGenericTypeArguments(runtimeClass, classOf[OperatorRuntime[_,_,_]].getName).get

  private[plugin] val runtimeInputClass = runtimeClassArgumentMap("I")
  private[plugin] val runtimeOutputClass = runtimeClassArgumentMap("O")
  private[plugin] val runtimeContextClass = runtimeClassArgumentMap("CTX")

  private[plugin] val inputsAreValid = guiNodeInputClass == runtimeInputClass
  private[plugin] val outputsAreValid = guiNodeOutputClass == runtimeOutputClass
}

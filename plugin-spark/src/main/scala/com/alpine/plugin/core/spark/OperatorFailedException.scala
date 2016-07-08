/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.spark

/**
  * To be thrown if the operator encounters some exception at runtime (like a bad configuration),
  * and the developer does not want the framework (in particular, Spark) to retry the operation.
  */
class OperatorFailedException(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)

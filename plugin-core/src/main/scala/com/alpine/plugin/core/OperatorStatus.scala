package com.alpine.plugin.core

import com.alpine.plugin.EmptyIOMetadata
import com.alpine.plugin.core.io.IOMetadata

/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
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

case class OperatorStatus(isValid: Boolean,
                          msg: Option[String],
                          outputMetadata: IOMetadata
                         ) {
  def this(isValid: Boolean, msg: Option[String]) = {
    this(isValid, msg, EmptyIOMetadata())
  }
}

object OperatorStatus {
  def apply(isValid: Boolean): OperatorStatus = {
    OperatorStatus(isValid = isValid, msg = None, EmptyIOMetadata())
  }

  def apply(isValid: Boolean, msg: String): OperatorStatus = {
    OperatorStatus(isValid = isValid, msg = Some(msg), EmptyIOMetadata())
  }

  def apply(isValid: Boolean, msg: Option[String]): OperatorStatus = {
    OperatorStatus(isValid, msg, EmptyIOMetadata())
  }
}

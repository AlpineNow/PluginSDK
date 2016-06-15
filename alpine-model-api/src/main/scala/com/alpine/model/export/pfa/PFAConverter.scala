/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
trait PFAConverter {

  /**
    * Must use the nameSpaceID as a suffix on any declared cells, types, fields, or functions.
    */
  def toPFAComponents(inputName: String = "input", nameSpaceID: Option[String] = None): PFAComponents

  def toJsonPFA: String = {
    toPFAComponents().toJson
  }

}

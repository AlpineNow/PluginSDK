/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa

import java.io.File

import com.alpine.model.export.pfa.avrotypes.RecordType
import com.alpine.model.export.pfa.expressions.{CellInit, PFAFunction}
import com.alpine.model.export.pfa.utils.JsonConverter

/**
  * Created by jenny on 5/26/16.
  */
case class PFAComponents(input: RecordType,
                         output: RecordType,
                         cells: Map[String, CellInit],
                         action: Seq[Any],
                         fcns: Map[String, PFAFunction] = Map.empty
                        ) {

  def toJson: String = {
    JsonConverter.anyToJson(this)
  }

  def writeToFile(file: File): Unit = {
    JsonConverter.writeAsJsonToFile(this, file)
  }

}

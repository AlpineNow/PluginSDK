/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.{PFAComponents, PFAConverter}
import com.alpine.model.pack.preprocess.RenamingModel

/**
  * Created by Jennifer Thompson on 1/5/17.
  */
class RenamingPFAConverter(model: RenamingModel) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)
    val outputType = AvroTypes.fromAlpineSchema("output", model.outputFeatures)

    PFAComponents(
      input = inputType,
      output = outputType,
      cells = Map.empty,
      action = Seq(inputName)
    )
  }
}

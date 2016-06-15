/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.pack.UnitModel

/**
  * Created by Jennifer Thompson on 6/1/16.
  */
class UnitPFAConverter(model: UnitModel) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.outputFeatures)
    val outputType = AvroTypes.fromAlpineSchema("output", model.outputFeatures)

    new PFAComponents(
      input = inputType,
      output = outputType,
      cells = Map.empty,
      action = Seq(inputName)
    )
  }
}

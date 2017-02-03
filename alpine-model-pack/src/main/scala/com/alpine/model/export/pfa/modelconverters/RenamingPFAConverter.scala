/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.expressions.NewPFAObject
import com.alpine.model.export.pfa.utils.ExpressionUtil.outputTypeFromAlpineSchema
import com.alpine.model.export.pfa.{PFAComponents, PFAConverter}
import com.alpine.model.pack.preprocess.RenamingModel

/**
  * Created by Jennifer Thompson on 1/5/17.
  */
class RenamingPFAConverter(model: RenamingModel) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)
    val outputContents = (model.outputFeatures zip model.inputFeatures).map {
      case (outputColumn, inputColumn) => (outputColumn.columnName, inputName + "." + inputColumn.columnName)
    }.toMap

    val action = NewPFAObject(outputContents, outputType)

    PFAComponents(
      input = inputType,
      output = outputType,
      cells = Map.empty,
      action = Seq(action)
    )
  }
}

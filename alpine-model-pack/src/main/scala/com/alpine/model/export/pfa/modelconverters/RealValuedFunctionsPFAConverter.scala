/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.expressions.NewPFAObject
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.export.pfa.{PFAComponents, PFAConverter}
import com.alpine.model.pack.preprocess._
import com.alpine.plugin.core.io.ColumnDef

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class RealValuedFunctionsPFAConverter(model: RealValuedFunctionsModel) extends PFAConverter {
  /**
    * Must use the nameSpaceID as a suffix on any declared cells, types, fields, or functions.
    */
  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val outputContents = (model.outputFeatures zip model.functions).map {
        case (colDef: ColumnDef, RealFunctionWithIndex(TypeWrapper(function), index)) =>
          val inputFeatureName: String = inputName + "." + model.inputFeatures(index).columnName
          val pfaFunction = function.pfaRepresentation(inputFeatureName)
          (colDef.columnName, pfaFunction)
      }.toMap

      Seq(new NewPFAObject(outputContents, outputType))
    }

    new PFAComponents(
      input = inputType,
      output = outputType,
      cells = Map.empty,
      action = action
    )
  }

}

/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.expressions.{FunctionExecute, NewPFAObject}
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.preprocess._
import com.alpine.plugin.core.io.ColumnDef

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class NullValueReplacementPFAConverter(model: NullValueReplacement) extends PFAConverter {
  /**
    * Must use the nameSpaceID as a suffix on any declared cells, types, fields, or functions.
    */
  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures, allowNullValues = true)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val outputContents = (model.outputFeatures zip model.replacementValues).map {
        case (ColumnDef(columnName, _), replacementValue: Any) =>
          // Input features have the same names as the output features.
          val inputFeatureName: String = inputName + "." + columnName
          val defaultValue = qualifyLiteralValue(replacementValue)
          val pfaFunction = FunctionExecute("impute.defaultOnNull", inputFeatureName, defaultValue)
          (columnName, pfaFunction)
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

/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.expressions.{FunctionExecute, NewPFAObject}
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.preprocess.MatrixModel
import com.alpine.plugin.core.io.ColumnDef

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class MatrixPFAConverter(model: MatrixModel) extends PFAConverter {
  /**
    * Must use the nameSpaceID as a suffix on any declared cells, types, fields, or functions.
    */
  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val outputContents = model.outputFeatures.zipWithIndex.map {
        case (colDef: ColumnDef, index: Int) =>
          val row = model.values(index)
          val multipliedValues = row.zipWithIndex.flatMap {
            case (coefficient, j) =>
              if (coefficient == 0) None
              else if (coefficient == 1) Some(inputName + "." + model.inputFeatures(j).columnName)
              else Some(FunctionExecute("*", inputName + "." + model.inputFeatures(j).columnName, coefficient))
          }
          (colDef.columnName, sum(multipliedValues))
      }.toMap

      Seq(NewPFAObject(outputContents, outputType))
    }

    PFAComponents(
      input = inputType,
      output = outputType,
      cells = Map.empty,
      action = action
    )
  }

}

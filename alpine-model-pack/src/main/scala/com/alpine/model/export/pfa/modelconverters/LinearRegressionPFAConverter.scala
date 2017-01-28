/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.{AvroTypes, FieldType, RecordType}
import com.alpine.model.export.pfa.expressions.{CellAccess, CellInit, FunctionExecute, NewPFAObject}
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.ml.LinearRegressionModel

/**
  * Created by Jennifer Thompson on 5/26/16.
  */
case class LinearRegressionPFAConverter(model: LinearRegressionModel) extends PFAConverter {

  def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {

    val modelCellName = appendNameSpaceID(nameSpaceID, "linearModel")

    val cells = Map(
      modelCellName -> CellInit(
        RecordType(
          "Model",
          Seq(FieldType("coeff", AvroTypes.arrayDouble), FieldType("const", AvroTypes.double))
        ),
        Map("coeff" -> model.coefficients, "const" -> model.intercept)
      )
    )

    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val convertToVector = let(
        "vector",
        recordAsArray(inputName, model.inputFeatures.map(_.columnName),  AvroTypes.double)
      )
      val linearRegression = let("prediction", FunctionExecute("model.reg.linear", "vector", CellAccess(modelCellName)))
      val record = NewPFAObject(
        Map(model.outputFeatures.head.columnName -> "prediction"),
        outputType
      )
      Seq(convertToVector, linearRegression, record)
    }

    PFAComponents(
      input = AvroTypes.fromAlpineSchema("input", model.inputFeatures),
      output = outputType,
      cells = cells,
      action = action
    )
  }

}

/*
  * Early version.
  *
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [0.9, 1, 5, -1]
      const: 3.4
action:
  model.reg.linear:
    - input
    - cell: model
*/

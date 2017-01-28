/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.{ArrayType, AvroTypes}
import com.alpine.model.export.pfa.expressions._
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.preprocess.OneHotEncodingModel

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
case class OneHotEncodingPFAConverter(model: OneHotEncodingModel) extends PFAConverter {

  def toPFAComponents(inputName: String, nameSpaceID: Option[String] = None): PFAComponents = {

    val oneHotCellName = appendNameSpaceID(nameSpaceID, "oneHot")
    val dropBaseValue = appendNameSpaceID(nameSpaceID, "dropBaseValue")

    val cells = Map(
      oneHotCellName -> CellInit(
        ArrayType(AvroTypes.arrayString),
        model.oneHotEncodedFeatures.map(f => f.hotValues ++ f.baseValue.toSeq)
      ),
      dropBaseValue -> CellInit(
        ArrayType(AvroTypes.boolean),
        model.oneHotEncodedFeatures.map(f => f.baseValue.isDefined)
      )
    )

    val expandFcnName = appendNameSpaceID(nameSpaceID, "expand")

    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val recordToArray = let(
        "arrayString",
        recordAsArray(inputName, model.inputFeatures.map(_.columnName), AvroTypes.string)
      )
      val expanding = let(
        "expanded",
        FunctionExecute("a.flatMapWithIndex", "arrayString", FcnRef(UDFAccess(expandFcnName))))

      val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

      Seq(
        recordToArray,
        expanding,
        arrayAsRecord("expanded", model.outputFeatures.map(_.columnName), outputType)
      )
    }

    val expandFunction = {
      val fanOutArray = let("fanOut",
        FunctionExecute(
          "cast.fanoutInt",
          "value",
          AttributeAccess(CellAccess(oneHotCellName), "i"),
          true
        )
      )
      val lastValue = FunctionExecute("a.last", "fanOut")
      val errorCheck = Map(
        "if" -> FunctionExecute("==", 1, lastValue),
        "then" -> DoExpression(Seq(
          Map("log" -> FunctionExecute(
            "s.concat",
            qualifyLiteralValue("[ERROR] Unrecognised value encountered in One-Hot Encoding: "),
            "value"
          )),
          Map("error" -> "Unrecognised value encountered in One-Hot Encoding. See log for more information.")
        ))
      )

      val result = Map(
        "if" -> AttributeAccess(CellAccess(dropBaseValue), "i"),
        "then" -> FunctionExecute("a.init", FunctionExecute("a.init", "fanOut")), // Drop the outOfRange indicator and the base value.
        "else" -> FunctionExecute("a.init", "fanOut") // Drop the outOfRange indicator.
      )

      new PFAFunction(
        params = Map("i" -> AvroTypes.int, "value" -> AvroTypes.string),
        ret = AvroTypes.arrayInt,
        `do` = Seq(fanOutArray, errorCheck, result))
    }

    val fcns = Map(expandFcnName -> expandFunction)

    PFAComponents(
      input = AvroTypes.fromAlpineSchema("input", model.inputFeatures),
      output = outputType,
      cells = cells,
      action = action,
      fcns = fcns
    )
  }

}

/**
  * Expect something like:
  * *
  * input: {type: array, items: string}
  * output: {type: array, items: int}
  * cells:
  * oneHot:
  * type: {type: array, items: {type: array, items: string}}
  * init: [["sunny","overcast"],["true"]]
  * action:
  * - a.flatMapWithIndex:
  * - input
  * - {fcn: u.expand}
  * fcns:
  * expand:
  * params: [{i: int}, {value: string}]
  * ret: {type: array, items: int}
  * do:
  * - cast.fanoutInt:
  * - value
  * - {attr: {cell: oneHot}, "path": [i]}
  * - false
  */

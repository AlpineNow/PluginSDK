/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.expressions.DoExpression
import com.alpine.model.export.pfa.utils.ExpressionUtil._

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
case class PipelinePFAConverter(modelPipeline: Seq[RowModel]) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String] = None): PFAComponents = {

    val inputNames = inputName :: modelPipeline.indices.map(i => appendNameSpaceID(nameSpaceID, "sub_input") + "_model_" + i).toList

    val (subPFAs, cells, fcns) = SharedPipelineCombinerLogic.getPFAs(modelPipeline, inputNames.dropRight(1), nameSpaceID)

    val subActions = subPFAs.map(p => p.action)

    val action: Seq[Any] = {

      val middleActions = (subActions zip inputNames.drop(1)).map {
        case (subAction, outputName) =>
          let(
            outputName,
            DoExpression(subAction) // Use "do" to reduce scope of declared variables.
          )
      }

      val lastAction: Any = inputNames.last
      middleActions ++ Seq(lastAction)
    }

    PFAComponents(
      input = subPFAs.head.input,
      output = subPFAs.last.output,
      cells = cells,
      action = action,
      fcns = fcns
    )
  }

}

/*
Expect something like:

input: {type: array, items: string}
output: double
cells:
  oneHot:
    type: {type: array, items: {type: array, items: string}}
    init: [["sunny", "rain"], ["yes"]]
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [0.9, 1.0, 5.0]
      const: 3.4
action:
  - let:
     vector:
       a.map:
        - {a.flatMapWithIndex:[input, {fcn: u.expand}]}
        - {fcn: "u.intToDouble"}
  - model.reg.linear:
    - vector
    - cell: model
fcns:
  # Need to declare this because it needs stricter typing of the input to be used with a.map.
  intToDouble:
    params: [{x: int}]
    ret: double
    do:
      - {"cast.double": [x]}
  expand:
    params: [{i: int}, {value: string}]
    ret: {type: array, items: int}
    do:
      - cast.fanoutInt:
        - value
        - {attr: {cell: oneHot}, "path": [i]}
        - false
 */

/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.AvroTypes
import com.alpine.model.export.pfa.expressions.{DoExpression, NewPFAObject}
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.multiple.CombinerModel

/**
  * Created by Jennifer Thompson on 6/1/16.
  */
class CombinerPFAConverter(model: CombinerModel) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String] = None): PFAComponents = {

    val inputNames = model.models.map(_ => inputName) // Use the same input for each model.
    val (subPFAs, cells, fcns) = SharedPipelineCombinerLogic.getPFAs(model.models.map(_.model), inputNames, nameSpaceID)

    val actions = subPFAs.map(p => p.action)
    val intermediateOutputNames = actions.indices.map(i => appendNameSpaceID(nameSpaceID, "sub_output") + "_model_" + i)

    // This is a hack. Need to address allowNullValues in compound models properly (also an issue in the SDK).
    val allowNullValues: Boolean = model.models.map(_.model).exists(_.transformer.allowNullValues)

    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures, allowNullValues)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val subModelOutputFeatures = model.models.map(_.model.outputFeatures)
    val completeOutputFeatures = model.outputFeatures

    val action: Seq[Any] = {

      val middleActions = actions.zipWithIndex.map {
        case (subAction, i) =>
          let(
            intermediateOutputNames(i),
            DoExpression(subAction) // Use "do" to reduce scope of declared variables.
          )
      }

      val namesToSelect = subModelOutputFeatures.indices.flatMap(i => {
        subModelOutputFeatures(i).map(c => {
          intermediateOutputNames(i) + "." + c.columnName
        })
      })

      val finalRecordContents = (completeOutputFeatures.map(_.columnName) zip namesToSelect).toMap

      val finalRecord = new NewPFAObject(finalRecordContents, outputType)

      middleActions ++ Seq(finalRecord)
    }

    new PFAComponents(
      input = inputType,
      output = outputType,
      cells = cells,
      action = action,
      fcns = fcns
    )
  }
}

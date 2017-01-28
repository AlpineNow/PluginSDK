/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.{AvroType, AvroTypes}
import com.alpine.model.export.pfa.expressions.DoExpression
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.multiple.GroupByModel

/**
  * Created by Jennifer Thompson on 6/9/16.
  */
class GroupByPFAConverter(model: GroupByModel[_ <: RowModel] with RowModel) extends PFAConverter {
  /**
    * Must use the nameSpaceID as a suffix on any declared cells, types, fields, or functions.
    */
  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {

    // This is a hack. Need to address allowNullValues in compound models properly (also an issue in the SDK).
    val allowNullValues: Boolean = model.modelsByGroup.values.exists(_.transformer.allowNullValues)

    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures, allowNullValues)
    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val inputNames = model.modelsByGroup.map(_ => inputName).toSeq
    val modelsByGroupAsSeq = model.modelsByGroup.toSeq // Want to preserve ordering.
    // Use the same input for each model.
    val (subPFAs, cells, fcns) = SharedPipelineCombinerLogic.getPFAs(modelsByGroupAsSeq.map(_._2), inputNames, nameSpaceID)

    val action = {
      val conditionContent: Seq[Any] = modelsByGroupAsSeq.zipWithIndex.tail.map {
        case ((key, subModel), index) =>
          val doExpression = doExpressionForSubModel(subPFAs(index), nameSpaceID, outputType)
          Map(
            "if" -> Map("==" -> Seq(inputName + "." + model.groupByFeature.columnName, qualifyLiteralValue(key))),
            "then" -> doExpression
          )
      }
      Map(
        "cond" -> conditionContent,
        // Have to have an else clause in order to have a return type.
        "else" -> doExpressionForSubModel(subPFAs.head, nameSpaceID, outputType)
      )
    }

    PFAComponents(
      input = inputType,
      output = outputType,
      cells = cells,
      action = Seq(action),
      fcns = fcns
    )
  }

  def doExpressionForSubModel(subPFA: PFAComponents, nameSpaceID: Option[String], outputType: AvroType): DoExpression = {
    // Have to redefine the return type name, so that all of the results have the same type.
    // Otherwise it results in a UnionType, which is impossible (as far as I can tell),
    // to turn back into a RecordType, even if all of the fields are identical.
    val subResultName: String = appendNameSpaceID(nameSpaceID, "subResult")
    DoExpression(Seq(
      let(subResultName, DoExpression(subPFA.action)),
      recordAsNewRecord(subResultName, model.outputFeatures.map(_.columnName), outputType)
    ))
  }
}

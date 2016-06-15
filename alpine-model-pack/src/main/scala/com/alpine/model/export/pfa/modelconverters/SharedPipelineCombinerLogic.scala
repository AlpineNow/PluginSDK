/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.PFAComponents
import com.alpine.model.export.pfa.expressions.{CellInit, PFAFunction}
import com.alpine.model.export.pfa.utils.ExpressionUtil._

/**
  * Created by Jennifer Thompson on 6/8/16.
  */
object SharedPipelineCombinerLogic {

  def getPFAs(models: Seq[RowModel], inputNames: Seq[String], nameSpaceID: Option[String]): (Seq[PFAComponents], Map[String, CellInit], Map[String, PFAFunction]) = {
    val subPFAs = models.zipWithIndex.map {
      case (m, index) =>
        val subModelNameSpaceID: String = prependNameSpaceID(nameSpaceID, index.toString)
        val inputName: String = inputNames(index)
        getPFAComponents(m, subModelNameSpaceID, inputName)
    }

    val cells = {
      val cellKeyNames: Seq[String] = subPFAs.flatMap(p => p.cells.keys)
      val duplicateCellKeyNames = duplicates(cellKeyNames)
      if (duplicateCellKeyNames.nonEmpty) {
        throw new Exception("More than one model contains the cell name(s): " + duplicateCellKeyNames.mkString(", "))
      }
      subPFAs.flatMap(p => p.cells).toMap
    }

    val fcns = {
      val fcnKeyNames: Seq[String] = subPFAs.flatMap(p => p.fcns.keys)
      val duplicateFcnKeyNames = duplicates(fcnKeyNames)
      if (duplicateFcnKeyNames.nonEmpty) {
        throw new Exception("More than one model contains the function name(s): " + duplicateFcnKeyNames.mkString(", "))
      }
      subPFAs.flatMap(p => p.fcns).toMap
    }

    (subPFAs, cells, fcns)
  }

  def getPFAComponents(m: RowModel, subModelNameSpaceID: String, inputName: String): PFAComponents = {
    val subPFA = ConverterLookup.findConverter(m)
      .toPFAComponents(
        inputName,
        nameSpaceID = Some(subModelNameSpaceID)
      )
    subPFA.cells.foreach {
      case (key, _) => if (!key.contains(subModelNameSpaceID)) {
        println("Warning: the cell with name " + key + " is incorrectly name-spaced (it is not suffixed with the model id). " +
          "This could result in name collisions with other models in the PFA document.")
      }
    }
    subPFA.fcns.foreach {
      case (key, _) => if (!key.contains(subModelNameSpaceID)) {
        println("Warning: the function with name " + key + " is incorrectly name-spaced (it is not suffixed with the model id). " +
          "This could result in name collisions with other models in the PFA document.")
      }
    }
    subPFA
  }

  def duplicates(values: Seq[String]): Set[String] = {
    values.groupBy(identity).collect { case (x, ys) if ys.size > 1 => x }.toSet
  }

}

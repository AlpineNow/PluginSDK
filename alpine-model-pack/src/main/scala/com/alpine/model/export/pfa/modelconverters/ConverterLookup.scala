/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}

import scala.util.Try

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
object ConverterLookup {

  def findConverter(model: RowModel): PFAConverter = {
    model match {
      case pfaConvertible: PFAConvertible => pfaConvertible.getPFAConverter
      case _ => throw new Exception("Model to PFA conversion not implemented for " + model.getClass)
    }
  }

  def tryToFindConverter(model: RowModel): Try[PFAConverter] = {
    Try(findConverter(model))
  }

}

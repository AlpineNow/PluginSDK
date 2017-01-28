/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.utils

import com.alpine.model.export.pfa.avrotypes._
import com.alpine.model.export.pfa.expressions.{FunctionExecute, LetExpression, NewPFAObject, SetExpression}
import com.alpine.plugin.core.io.ColumnDef

/**
  * Created by Jennifer Thompson on 5/31/16.
  */
object ExpressionUtil {

  def let(fieldName: String, value: Any): LetExpression = {
    new LetExpression(fieldName, value)
  }

  def set(fieldName: String, value: Any): SetExpression = {
    new SetExpression(fieldName, value)
  }

  def recordAsArray(inputName: String, valuesToSelect: Seq[String], itemType: AvroType): NewPFAObject = {
    NewPFAObject(
      valuesToSelect.map(v => inputName + "." + v),
      ArrayType(itemType)
    )
  }

  def arrayAsMap(inputName: String, fieldNames: Seq[String], itemType: AvroType): NewPFAObject = {
    NewPFAObject(
      fieldNames.zipWithIndex.map {
        case (name, i) =>
          (name, inputName + "." + i)
      }.toMap,
      MapType(itemType)
    )
  }

  def arrayAsRecord(inputName: String, fieldNames: Seq[String], recordType: AvroType): NewPFAObject = {
    NewPFAObject(
      fieldNames.zipWithIndex.map {
        case (name, i) =>
          (name, inputName + "." + i)
      }.toMap,
      recordType
    )
  }

  def recordAsNewRecord(inputName: String, fieldNames: Seq[String], recordType: AvroType): NewPFAObject = {
    NewPFAObject(
      fieldNames.map {
        case (name) =>
          (name, inputName + "." + name)
      }.toMap,
      recordType
    )
  }

  def outputTypeFromAlpineSchema(nameSpaceID: Option[String], outputFeatures: Seq[ColumnDef]): RecordType = {
    AvroTypes.fromAlpineSchema(appendNameSpaceID(nameSpaceID, "output"), outputFeatures)
  }

  def prependNameSpaceID(nameSpaceID: Option[String], s: String): String = {
    nameSpaceID match {
      case Some(t) => t + "_" + s
      case None => s
    }
  }

  def appendNameSpaceID(nameSpaceID: Option[String], s: String): String = {
    nameSpaceID match {
      case Some(t) => s + "_" + t
      case None => s
    }
  }

  def multiply(values: Seq[Any]): Any = {
    if (values.isEmpty) {
      1
    } else if (values.length == 1) {
      values.head
    } else {
      FunctionExecute("*", values.head, multiply(values.tail))
    }
  }

  def sum(values: Seq[Any]): Any = {
    if (values.isEmpty) {
      0
    } else if (values.length == 1) {
      values.head
    } else {
      FunctionExecute("+", values.head, sum(values.tail))
    }
  }

  def qualifyLiteralValue(a: Any): Any = {
    a match {
      // Strings have to be qualified to avoid confusion with variable names.
      case string: String => Map("string" -> string)
      case _ => a
    }
  }

}

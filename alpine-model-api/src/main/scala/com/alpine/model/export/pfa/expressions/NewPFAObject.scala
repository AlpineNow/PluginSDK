/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.expressions

import com.alpine.model.export.pfa.avrotypes.AvroType

/**
  * Place to put commonly used expressions.
  */

/**
  * Initialize a new Map or Record.
  */
case class NewPFAObject(`new`: Any, `type`: AvroType)

/**
  * Initialize a cell.
  *
  * @param `type` Type of the cell
  * @param `init` Contents of the cell
  */
case class CellInit(`type`: AvroType, `init`: Any)

/**
  * Access value from a cell.
  *
  * @param cell Name of the cell (the key to access it from the map).
  */
case class CellAccess(cell: String)

/**
  * Create a new function (either anonymously in an action, or declared in the fcns field).
  *
  * @param params Seq of name-to-type parameter maps (one per parameter).
  * @param ret    Return type of the function.
  * @param `do`   Logic for the function to execute.
  */
case class PFAFunction(params: Seq[Map[String, AvroType]], ret: AvroType, `do`: Any) {
  /**
    * It's tedious to make a separate map for each parameter, so use this constructor instead.
    *
    * @param params Map from parameter names to types.
    * @param ret    Return type of the function.
    * @param `do`   Logic for the function to execute.
    */
  def this(params: Map[String, AvroType], ret: AvroType, `do`: Any) {
    this(params.map(tuple => Map(tuple)).toSeq, ret, `do`)
  }
}

case class LetExpression(let: Map[String, Any]) {
  def this(fieldName: String, value: Any) = {
    this(Map(fieldName -> value))
  }
}

case class SetExpression(set: Map[String, Any]) {
  def this(fieldName: String, value: Any) = {
    this(Map(fieldName -> value))
  }
}

case class DoExpression(`do`: Any)

case class FunctionExecute(raw: Map[String, Seq[Any]]) extends PFAExpression

object FunctionExecute {
  def apply(functionName: String, singleArg: Any): FunctionExecute = {
    FunctionExecute(Map(functionName -> Seq(singleArg)))
  }

  def apply(functionName: String, firstArg: Any, secondArg: Any): FunctionExecute = {
    FunctionExecute(Map(functionName -> Seq(firstArg, secondArg)))
  }

  def apply(functionName: String, firstArg: Any, secondArg: Any, thirdArg: Any): FunctionExecute = {
    FunctionExecute(Map(functionName -> Seq(firstArg, secondArg, thirdArg)))
  }
}

case class AttributeAccess(attr: Any, path: Seq[Any])

object AttributeAccess {
  def apply(attr: Any, property: Any) = new AttributeAccess(attr, Seq(property))
}

case class FcnRef(fcn: String)

object UDFAccess {
  def apply(name: String): String = "u." + name
}

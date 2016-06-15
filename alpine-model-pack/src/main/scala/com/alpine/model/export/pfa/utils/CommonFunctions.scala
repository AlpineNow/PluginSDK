/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.utils

import com.alpine.model.export.pfa.avrotypes.{ArrayType, AvroType, AvroTypes, MapType}
import com.alpine.model.export.pfa.expressions.{FunctionExecute, PFAFunction}

object CommonFunctions {
  val intToDouble = new PFAFunction(
    params = Map("x" -> AvroTypes.int),
    ret = AvroTypes.double,
    `do` = FunctionExecute("cast.double", "x")
  )

  val zipDoubleMap = zipArraysToMap(AvroTypes.double)

  def zipArraysToMap(valueType: AvroType): PFAFunction = {
    val resultType: MapType = new MapType(valueType)
    new PFAFunction(
      params = Map("keys" -> AvroTypes.arrayString, "values" -> new ArrayType(valueType)),
      ret = resultType,
      `do` = Seq(
        FunctionExecute("map.join",
          FunctionExecute("a.zipmap",
            "keys", "values",
            new PFAFunction(
              Map("k" -> AvroTypes.string, "v" -> valueType),
              resultType,
              FunctionExecute(
                functionName = "map.add",
                firstArg = Map("type" -> resultType, "value" -> Map()),
                secondArg = "k",
                thirdArg = "v"
              )
            )
          )
        )
      )
    )
  }
}

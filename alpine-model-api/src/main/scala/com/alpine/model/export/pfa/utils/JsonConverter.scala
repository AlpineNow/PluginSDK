/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.utils

import java.io.File
import java.lang.reflect.Type

import com.alpine.common.serialization.json.JsonUtil
import com.alpine.model.export.pfa.expressions.PFAExpression
import com.google.gson.{JsonElement, JsonSerializationContext, JsonSerializer}

/**
  * Created by Jennifer Thompson on 5/27/16.
  */
object JsonConverter {

  private val gson = JsonUtil
    .simpleGsonBuilder()
    .disableHtmlEscaping() // Needed so that ">" shows up literally instead of in unicode.
    .registerTypeHierarchyAdapter(classOf[PFAExpression], new PFAExpressionAdapter())
    .setPrettyPrinting()
    .serializeNulls() // Useful for generating test input records.
    .create()

  def anyToJson(any: Any): String = {
    gson.toJson(any)
  }

  def writeAsJsonToFile(obj: Any, file: File): Unit = {
    JsonUtil.writeToFile(gson)(obj, obj.getClass, file, writePrettily = true)
  }

}

class PFAExpressionAdapter extends JsonSerializer[PFAExpression] {

  override def serialize(t: PFAExpression, `type`: Type, jsonSerializationContext: JsonSerializationContext): JsonElement = {
    jsonSerializationContext.serialize(t.raw)
  }
}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import java.lang.reflect.{ParameterizedType, Type}

import com.google.gson._

/**
 * Gson adapter to be used to serialize and serialize scala.Option to and from JSON
 * with Gson.
 */
case class GsonOptionAdapter() extends JsonSerializer[Option[_]] with JsonDeserializer[Option[_]] {

  private val someStr = "Some"
  private val noneStr = "None"

  override def serialize(obj: Option[_], t: Type, jsc: JsonSerializationContext): JsonElement = {
    if (obj.isDefined) {
      val content = obj.get
      val jsonContent = jsc.serialize(content, getFirstTypeParameter(t))
      val resultObj: JsonObject = new JsonObject
      resultObj.add(JsonUtil.typeKey, jsc.serialize(someStr))
      resultObj.add(JsonUtil.dataKey, jsonContent)
      resultObj
    } else {
      val resultObj: JsonObject = new JsonObject
      resultObj.add(JsonUtil.typeKey, jsc.serialize(noneStr))
      resultObj
    }
  }

  override def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): Option[_] = {
    val jsonObj: JsonObject = jsonElement.getAsJsonObject
    val valueType = jsonObj.get(JsonUtil.typeKey).getAsString
    if (someStr == valueType) {
      val value: JsonElement = jsonObj.get(JsonUtil.dataKey)
      Some(jdc.deserialize(value, getFirstTypeParameter(t)))
    } else {
      None
    }
  }

  def getFirstTypeParameter(p: Type): Type = {
    p match {
      case t: ParameterizedType =>
        val actualTypeArguments = t.getActualTypeArguments
        if (actualTypeArguments.length == 1) {
          actualTypeArguments.apply(0)
        } else classOf[Any]
      case _ => classOf[Any]
    }
  }

}

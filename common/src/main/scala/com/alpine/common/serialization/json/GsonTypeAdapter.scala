/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.common.serialization.json

import java.lang.reflect.Type

import com.google.gson._

/**
 * Gson adapter to be used to serialize and serialize [[TypeWrapper]] to and from JSON
 * with Gson.
 *
 * To be used when the developer wishes to to include a interface type in the model object,
 * or one of its fields.
 *
 * e.g. to store a List[Bird], or just val b: Bird, where
 * trait Bird
 * class Robin() extends Bird
 * class Eagle() extends Bird
 * the Bird type should be wrapped in TypeWrapper:
 * case class Aviary(specialBird: TypeWrapper[Bird], commonBirds: List[TypeWrapper[Bird]])
 */
class GsonTypeAdapter(classLoaderUtil: Option[ClassLoaderUtil] = None) extends JsonSerializer[TypeWrapper[_]] with JsonDeserializer[TypeWrapper[_]] {

  private val helper = new ClassSerializer(classLoaderUtil)

  @throws(classOf[JsonParseException])
  def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): TypeWrapper[_] = {
    val jsonObj: JsonObject = jsonElement.getAsJsonObject
    val valueClass: JsonElement = jsonObj.get(JsonUtil.typeKey)
    val className: String = valueClass.getAsString
    val value: JsonElement = jsonObj.get(JsonUtil.dataKey)
    try {
      val clz = helper.getTypeForKey(className)
      val newValue: Any = jdc.deserialize(value, clz)
      new TypeWrapper(newValue)
    } catch {
      case e: ClassNotFoundException => throw new JsonParseException(e)
    }
  }

  def serialize(obj: TypeWrapper[_], t: Type, jdc: JsonSerializationContext): JsonElement = {
    val resultObj: JsonObject = new JsonObject
    val valueType: Class[_] = obj.value.getClass
    val jsonEle2: JsonElement = jdc.serialize(helper.getKeyForType(valueType))
    resultObj.add(JsonUtil.typeKey, jsonEle2)
    val jsonEle: JsonElement = jdc.serialize(obj.value, valueType)
    resultObj.add(JsonUtil.dataKey, jsonEle)
    resultObj
  }
}





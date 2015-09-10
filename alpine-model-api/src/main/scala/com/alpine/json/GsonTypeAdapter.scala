/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

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
class GsonTypeAdapter(typeHints: TypeHints = EmptyTypeHints()) extends JsonSerializer[TypeWrapper[_]] with JsonDeserializer[TypeWrapper[_]] {

  @throws(classOf[JsonParseException])
  def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): TypeWrapper[_] = {
    val jsonObj: JsonObject = jsonElement.getAsJsonObject
    val valueClass: JsonElement = jsonObj.get(JsonUtil.typeKey)
    val className: String = valueClass.getAsString
    val value: JsonElement = jsonObj.get(JsonUtil.dataKey)
    try {
      val clz = typeHints.getTypeForKey(className)
      val newValue: Any = jdc.deserialize(value, clz)
      new TypeWrapper(newValue)
    } catch {
      case e: ClassNotFoundException => throw new JsonParseException(e)
    }
  }

  def serialize(obj: TypeWrapper[_], t: Type, jdc: JsonSerializationContext): JsonElement = {
    val resultObj: JsonObject = new JsonObject
    val valueType: Class[_] = obj.value.getClass
    val jsonEle2: JsonElement = jdc.serialize(typeHints.getKeyForType(valueType))
    resultObj.add(JsonUtil.typeKey, jsonEle2)
    val jsonEle: JsonElement = jdc.serialize(obj.value, valueType)
    resultObj.add(JsonUtil.dataKey, jsonEle)
    resultObj
  }
}

/**
 * To be used as a wrapper for interfaces.
 * It includes the actual class name in the serialization in order
 * to instantiate the new object on deserialization.
 * @param value Actual value to serialize.
 * @tparam T Type of value.
 */
case class TypeWrapper[T](value: T) {
  val valueClass: Class[_ <: T] = value.getClass
}

trait TypeHints {
  def getKeyForType(t: Class[_]): String
  def getTypeForKey(x: String): Type
}

case class EmptyTypeHints() extends TypeHints {
  override def getKeyForType(t: Class[_]): String = t.getCanonicalName

  override def getTypeForKey(x: String): Type = Class.forName(x)
}
/**
 * For known types, we store the type in JSON by a simple String key, which we map to and form the class.
 * For types not in the type map, we use the class path as the key, and use Class.forName(name) to get the class.
 */
case class KnownTypes(typeToKeyMap: Map[Class[_], String]) extends TypeHints  {

  lazy val keyToTypeMap = typeToKeyMap.map(_.swap)

  def getKeyForType(t: Class[_]): String = {
    typeToKeyMap.get(t) match {
      case Some(x: String) => x
      case None => t.getCanonicalName
    }
  }

  def getTypeForKey(x: String): Type = {
    keyToTypeMap.get(x) match {
      case Some(t: Type) => t
      case None => Class.forName(x)
    }
  }
}


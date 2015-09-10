/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import java.lang.reflect.{ParameterizedType, Type}

import com.google.gson._

/**
 * Contains all functionality to com.alpine.miner.workflow.operator.plugin10.serialization.InterfaceAdapter.
 * As extra, we can take a TypeHints argument, so we can use more readable types in the serialization instead of
 * the full class names (which we use if a class is not found in TypeHints).
 * (We may consolidate the code later).
 * For serializing/deserializing arbitrary interface types.
 * From stackoverflow with some modifications to handle generic
 * interfaces.
 */
class GsonInterfaceAdapter[T](typeHints: TypeHints = EmptyTypeHints()) extends JsonSerializer[T] with JsonDeserializer[T] {
  def serialize(`object`: T, interfaceType: Type, context: JsonSerializationContext): JsonElement = {
    val wrapper: JsonObject = new JsonObject
    val objectType: Type = `object`.getClass
    var implType: Type = objectType
    if (interfaceType.isInstanceOf[ParameterizedType]) {
      val finalInfType: Type = interfaceType
      val finalObjType: Type = objectType
      implType = new ParameterizedType() {
        def getActualTypeArguments: Array[Type] = {
          finalInfType.asInstanceOf[ParameterizedType].getActualTypeArguments
        }

        def getRawType: Type = {
          finalObjType
        }

        def getOwnerType: Type = {
          null
        }
      }
    }
    // Differ from plugin version here.
    val typeSerialization: String = typeHints.getKeyForType(`object`.getClass)
    wrapper.addProperty(JsonUtil.typeKey, typeSerialization)
    wrapper.add(JsonUtil.dataKey, context.serialize(`object`, implType))
    wrapper
  }

  def deserialize(elem: JsonElement, interfaceType: Type, context: JsonDeserializationContext): T = {
    val wrapper: JsonObject = elem.asInstanceOf[JsonObject]
    val typeName: JsonElement = get(wrapper, JsonUtil.typeKey)
    val data: JsonElement = get(wrapper, JsonUtil.dataKey)
    var actualType: Type = typeForName(typeName)
    if (interfaceType.isInstanceOf[ParameterizedType]) {
      val finalInfType: Type = interfaceType
      val finalObjType: Type = actualType
      actualType = new ParameterizedType() {
        def getActualTypeArguments: Array[Type] = {
          finalInfType.asInstanceOf[ParameterizedType].getActualTypeArguments
        }

        def getRawType: Type = {
          finalObjType
        }

        def getOwnerType: Type = {
          null
        }
      }
    }
    context.deserialize(data, actualType)
  }

  private def typeForName(typeElem: JsonElement): Type = {
    try {
      // Differ from plugin version here.
      typeHints.getTypeForKey(typeElem.getAsString)
    }
    catch {
      case e: ClassNotFoundException => {
        throw new JsonParseException(e)
      }
    }
  }

  private def get(wrapper: JsonObject, memberName: String): JsonElement = {
    val elem: JsonElement = wrapper.get(memberName)
    if (elem == null) {
      throw new JsonParseException("no '" + memberName + "' member found in what was expected to be an interface wrapper")
    }
    elem
  }
}

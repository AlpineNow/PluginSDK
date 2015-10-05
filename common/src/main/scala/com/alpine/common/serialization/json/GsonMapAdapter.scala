/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.common.serialization.json

import java.lang.reflect.{ParameterizedType, Type}
import java.util

import com.google.gson._
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

/**
 * Gson adapter used to serialize and serialize [[scala.collection.Map]] to and from JSON.
 */
class GsonMapAdapter extends JsonSerializer[Map[_,_]] with JsonDeserializer[Map[_,_]] {
  import scala.collection.JavaConverters._

  @throws(classOf[JsonParseException])
  def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): Map[_, _] = {
    val javaMap: java.util.Map[_, _] = jdc.deserialize(jsonElement, scalaMapTypeToJava(t))
    javaMap.asScala.toMap
  }

  def serialize(obj: Map[_,_], t: Type, jdc: JsonSerializationContext): JsonElement = {
    jdc.serialize(obj.asInstanceOf[Map[Any,Any]].asJava, scalaMapTypeToJava(t))
  }

  private def scalaMapTypeToJava(t: Type): Type = {
    t match {
      case pType: ParameterizedType => parameterizedMapTypeToJava(pType)
      case _ => classOf[util.Map[_, _]]
    }
  }

  private def parameterizedMapTypeToJava(t: ParameterizedType): ParameterizedType = {
    ParameterizedTypeImpl.make(classOf[java.util.Map[_,_]], t.getActualTypeArguments, null)
  }
}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import java.lang.reflect.{ParameterizedType, Type}
import java.util

import com.google.gson._
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

/**
 * Gson adapter to be used to serialize and serialize [[scala.collection.Seq]] to and from JSON
 * with Gson.
 */
case class GsonSeqAdapter() extends JsonSerializer[Seq[_]] with JsonDeserializer[Seq[_]] {
  import scala.collection.JavaConverters._

  @throws(classOf[JsonParseException])
  def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): Seq[_] = {
    val p = scalaListTypeToJava(t)
    val javaList: java.util.List[_] = jdc.deserialize(jsonElement, p)
    javaList.asScala.toList
  }

  def serialize(obj: Seq[_], t: Type, jdc: JsonSerializationContext): JsonElement = {
    val p = scalaListTypeToJava(t)
    jdc.serialize(obj.asInstanceOf[Seq[Any]].asJava, p)
  }

  private def scalaListTypeToJava(t: Type): Type = {
    t match {
      case pType: ParameterizedType => parameterizedListTypeToJava(pType)
      case _ => classOf[util.List[_]]
    }
  }

  private def parameterizedListTypeToJava(t: ParameterizedType): ParameterizedType = {
    ParameterizedTypeImpl.make(classOf[java.util.List[_]], t.getActualTypeArguments, null)
  }
}

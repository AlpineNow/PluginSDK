package com.alpine.common.serialization.json

import java.lang.reflect.{ParameterizedType, Type}
import java.util

import com.google.gson._
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

case class GsonSetAdapter() extends JsonSerializer[Set[_]] with JsonDeserializer[Set[_]] {

  import scala.collection.JavaConverters._

  @throws(classOf[JsonParseException])
  def deserialize(jsonElement: JsonElement, t: Type, jdc: JsonDeserializationContext): Set[_] = {
    val p = scalaSetTypeToJava(t)
    val javaSet: java.util.Set[_] = jdc.deserialize(jsonElement, p)
    javaSet.asScala.toSet
  }

  def serialize(obj: Set[_], t: Type, jdc: JsonSerializationContext): JsonElement = {
    val p = scalaSetTypeToJava(t)
    jdc.serialize(obj.asInstanceOf[Set[Any]].asJava, p)
  }

  private def scalaSetTypeToJava(t: Type): Type = {
    t match {
      case pType: ParameterizedType => parameterizedListTypeToJava(pType)
      case _ => classOf[util.Set[_]]
    }
  }

  private def parameterizedListTypeToJava(t: ParameterizedType): ParameterizedType = {
    ParameterizedTypeImpl.make(classOf[java.util.Set[_]], t.getActualTypeArguments, null)
  }
}

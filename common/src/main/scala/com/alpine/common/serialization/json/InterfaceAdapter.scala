package com.alpine.common.serialization.json

import java.lang.reflect.{ParameterizedType, Type}

import com.google.gson._
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

/**
 * For serializing/deserializing arbitrary interface types.
 * From stackoverflow with some modifications to handle generic
 * interfaces.
 */
class InterfaceAdapter[T](classLoaderUtil: Option[_ <: ClassLoaderUtil] = None)
  extends JsonSerializer[T] with JsonDeserializer[T] {

  private val helper = new ClassSerializer(classLoaderUtil)

  def serialize(`object`: T, interfaceType: Type, context: JsonSerializationContext): JsonElement = {
    val wrapper: JsonObject = new JsonObject
    val rawType = `object`.getClass
    val completeType: Type = getCompleteType(rawType, interfaceType)
    val typeSerialization: String = helper.getKeyForType(`object`.getClass)
    wrapper.addProperty(JsonUtil.typeKey, typeSerialization)
    wrapper.add(JsonUtil.dataKey, context.serialize(`object`, completeType))
    wrapper
  }

  def deserialize(elem: JsonElement, interfaceType: Type, context: JsonDeserializationContext): T = {
    val wrapper: JsonObject = elem.asInstanceOf[JsonObject]
    val typeName: JsonElement = get(wrapper, JsonUtil.typeKey)
    val data: JsonElement = get(wrapper, JsonUtil.dataKey)
    val rawType = helper.getTypeForKey(typeName.getAsString)
    val completeType: Type = getCompleteType(rawType, interfaceType)
    context.deserialize(data, completeType)
  }

  /**
   * Takes parameter types from the interfaceType (if it is parameterized), and attaches them to the rawType.
   */
  private def getCompleteType(rawType: Class[_], interfaceType: Type): Type = {
    if (interfaceType.isInstanceOf[ParameterizedType]) {
      ParameterizedTypeImpl.make(rawType, interfaceType.asInstanceOf[ParameterizedType].getActualTypeArguments, null)
    } else {
      rawType
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

object InterfaceAdapter {

  def apply[T](classLoaderUtil: ClassLoaderUtil) = {
    new InterfaceAdapter[T](Some(classLoaderUtil))
  }
}

trait ClassLoaderUtil {
  def getExportTypeClassLoader(className: String) : Option[ClassLoader]
}

private class ClassSerializer(classLoaderUtil: Option[ClassLoaderUtil]) {

  @throws(classOf[JsonParseException])
  def getTypeForKey(x: String): Class[_] = {
    try {
      Class.forName(x)
    }
    catch {
      case e: ClassNotFoundException =>
        val className: String = x
        var clazz: Class[_] = null
        if (classLoaderUtil.nonEmpty) {
          val classLoader: Option[ClassLoader] = classLoaderUtil.get.getExportTypeClassLoader(className)
          if (classLoader.isDefined) {
            try {
              clazz = classLoader.get.loadClass(className)
            }
            catch {
              case e2: ClassNotFoundException =>
                throw new JsonParseException(e2)
            }
          }
        }
        if (clazz == null) {
          throw new JsonParseException(e)
        }
        clazz
    }
  }

  def getKeyForType(t: Class[_]): String = {
    t.getName
  }
}

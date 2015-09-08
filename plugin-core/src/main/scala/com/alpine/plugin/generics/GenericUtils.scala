/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.generics

import java.lang.reflect.{TypeVariable, ParameterizedType, Type}

import scala.collection.mutable

/**
 * Used to find generic type information.
 */
object GenericUtils {
  /**
   * Given a class with a generically typed ancestor, find out what the type arguments
   * were for a particular ancestor. This is needed if you have deep hierarchies with
   * multiple generic ancestors.
   * @param clazz The class whose ancestors we want to look at.
   * @param ancestorClassName The name of the ancestor class that we are trying to
   *                          get the type information about.
   * @return An optional map type argument values of the intended ancestor. None, if
   *         the ancestor was not found.
   */
  def getAncestorClassGenericTypeArguments(
    clazz: Type,
    ancestorClassName: String): Option[mutable.Map[String, Type]] = {
    var curType = clazz
    var returnMap: Option[mutable.Map[String, Type]] = None
    // The following is used to keep track of the map of generic type
    // declarations to actual instantiations. E.g., if a class is defined as
    // class A[T] and its child is declared as class B extends A[Int], then
    // we'll have a map of "T" -> Class[Int].
    var prevGenericTypeMap = mutable.Map[String, Type]()
    while (curType != null) {
      if (!curType.isInstanceOf[ParameterizedType]) {
        curType = curType.asInstanceOf[Class[_]].getGenericSuperclass
      } else {
        val curParameterizedType = curType.asInstanceOf[ParameterizedType]
        val curClass = curParameterizedType.getRawType.asInstanceOf[Class[_]]
        val genericTypeVars = curClass.getTypeParameters
        val genericTypeArgs = curParameterizedType.getActualTypeArguments
        val curGenericTypeMap = mutable.Map[String, Type]()
        var i = 0
        while (i < genericTypeArgs.length) {
          val genericTypeArg = genericTypeArgs(i)
          if (genericTypeArg.isInstanceOf[TypeVariable[_]]) {
            val actualTypeArg = prevGenericTypeMap(
              genericTypeArg.asInstanceOf[TypeVariable[_]].getName
            )
            curGenericTypeMap.put(genericTypeVars(i).getName, actualTypeArg)
          } else {
            curGenericTypeMap.put(genericTypeVars(i).getName, genericTypeArg)
          }

          i += 1
        }

        prevGenericTypeMap = curGenericTypeMap
        curType = curClass.getGenericSuperclass

        if (curClass.getCanonicalName.equals(ancestorClassName)) {
          returnMap = Some(curGenericTypeMap)
          curType = null
        }
      }
    }

    returnMap
  }
}

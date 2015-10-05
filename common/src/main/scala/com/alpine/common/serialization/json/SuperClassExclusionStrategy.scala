package com.alpine.common.serialization.json

import java.lang.reflect.Field

import com.google.gson.{FieldAttributes, ExclusionStrategy}

/**
 * Taken from Stackoverflow (and translated to scala).
 * http://stackoverflow.com/questions/16476513/class-a-declares-multiple-json-fields
 */
class SuperClassExclusionStrategy extends ExclusionStrategy {
  def shouldSkipClass(arg0: Class[_]): Boolean = {
    false
  }

  def shouldSkipField(fieldAttributes: FieldAttributes): Boolean = {
    val fieldName: String = fieldAttributes.getName
    val theClass: Class[_] = fieldAttributes.getDeclaringClass
    isFieldInSuperclass(theClass, fieldName)
  }

  private def isFieldInSuperclass(subclass: Class[_], fieldName: String): Boolean = {
    var superclass: Class[_] = subclass.getSuperclass
    var field: Field = null
    while (superclass != null) {
      field = getField(superclass, fieldName)
      if (field != null) return true
      superclass = superclass.getSuperclass
    }
    false
  }

  private def getField(theClass: Class[_], fieldName: String): Field = {
    try {
      theClass.getDeclaredField(fieldName)
    }
    catch {
      case e: Exception =>
        null
    }
  }
}

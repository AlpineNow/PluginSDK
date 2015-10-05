/*
 * *
 *  * COPYRIGHT (C) year Alpine Data Labs Inc. All Rights Reserved.
 *
 */

package com.alpine.common.serialization.json

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

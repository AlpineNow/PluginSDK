/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.util

import java.util

import scala.collection.JavaConversions

/**
 * These conversions are difficult to do in java, due to use of implicit parameters.
 * So we provide them here.
 */
object JavaConversionUtils {

  def toImmutableMap[U,V](m: java.util.Map[U, V]): Map[U,V] = {
    JavaConversions.mapAsScalaMap(m).toMap
  }
  
  def toImmutableMap[U,V](m: collection.mutable.Map[U, V]): Map[U,V] = {
    m.toMap
  }

  def reverseMap[U,V](m: Map[U, V]): Map[V, U] = {
    m.map(t => t.swap)
  }

  /**
   * Builds a scala list natively in Java.
   * @param javaList  objects or collection
   * @return A Scala list containing args
   */
  def toSeq[T](javaList : java.util.List[T]): Seq[T] = {
    JavaConversions.asScalaBuffer(javaList)
  }

  def toSeq[T](array : Array[T]): Seq[T] = {
    array
  }

  def toList[T](javaList: java.util.List[T]): List[T] = {
    javaList.toArray.toList.asInstanceOf[List[T]]
  }

  def toList[T](scalaList: Seq[T]): util.Collection[T] = {
    JavaConversions.asJavaCollection(scalaList)
  }

  /**
   * Build a scala sequence natively in Java.
   * Using the syntax : Seq(a1, a2, a3... )
   * @param args Java objects or collections
   */
  def scalaSeq[T](args: T*): Seq[T] = {
    args
  }

  /**
   * Utility Function to create the scala option type None in Java
   */
  def None[T]: Option[T] = {
    Option.empty
  }

  /**
   * Utility function to create scala option type Some(Value) in Java.
   */
  def Some[T](value: T): Option[T] = {
    Option.apply(value)
  }

  def getOrElse[A](option: Option[A], alternative: A): A = {
    option.getOrElse(alternative)
  }

}

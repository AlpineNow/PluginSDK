/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.util

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

}

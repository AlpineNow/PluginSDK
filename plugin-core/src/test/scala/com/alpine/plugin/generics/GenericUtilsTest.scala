/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.generics

import org.scalatest.FunSuite

object GenericUtilsTest {
  class A[A1, A2, A3] {}
  class B[B1, B2] extends A[B1, B2, ATest] {}
  class C[C1] extends B[C1, BTest] {}
  class D extends C[CTest] {}

  class CTest
  class BTest
  class ATest
}

/**
 * Tests for generic utils.
 */
class GenericUtilsTest extends FunSuite {
  test("Test ancestor generic type determination.") {
    val clazz = classOf[GenericUtilsTest.D]
    val cTypes =
      GenericUtils.getAncestorClassGenericTypeArguments(
        clazz,
        "com.alpine.plugin.generics.GenericUtilsTest.C"
      )

    assert(cTypes.get("C1").equals(classOf[GenericUtilsTest.CTest]))

    val bTypes =
      GenericUtils.getAncestorClassGenericTypeArguments(
        clazz,
        "com.alpine.plugin.generics.GenericUtilsTest.B"
      )

    assert(bTypes.get("B1").equals(classOf[GenericUtilsTest.CTest]))
    assert(bTypes.get("B2").equals(classOf[GenericUtilsTest.BTest]))

    val aTypes =
      GenericUtils.getAncestorClassGenericTypeArguments(
        clazz,
        "com.alpine.plugin.generics.GenericUtilsTest.A"
      )

    assert(aTypes.get("A1").equals(classOf[GenericUtilsTest.CTest]))
    assert(aTypes.get("A2").equals(classOf[GenericUtilsTest.BTest]))
    assert(aTypes.get("A3").equals(classOf[GenericUtilsTest.ATest]))

    val invalid =
      GenericUtils.getAncestorClassGenericTypeArguments(
        clazz,
        "com.alpine.plugin.generics.GenericUtilsTest.Bad"
      )

    assert(invalid.isEmpty)
  }
}

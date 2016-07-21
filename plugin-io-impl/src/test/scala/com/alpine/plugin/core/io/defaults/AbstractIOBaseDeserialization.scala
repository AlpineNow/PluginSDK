/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.io.defaults

import java.lang.reflect.Type

import com.alpine.common.serialization.json.JsonUtil
import com.alpine.plugin.core.io.IOBase
import com.alpine.plugin.core.serialization.json.CoreJsonUtil
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 7/20/16.
  */
trait AbstractIOBaseDeserialization extends FunSuite {

  val gson = JsonUtil.gsonBuilderWithInterfaceAdapters(CoreJsonUtil.typesNeedingInterfaceAdapters).create

  def testSerialization[T <: IOBase](ioBase: T, previousJson: Option[String] = None, t: Type = classOf[IOBase]): T = {
    val json = gson.toJson(ioBase, t)
    val newObj: IOBase = gson.fromJson(json, t)

    assert(ioBase === newObj)
    assert(ioBase.addendum === newObj.addendum)
    previousJson match {
      case Some(s) =>
        val oldObjDeserialized: IOBase = gson.fromJson(previousJson.get, t)
        assert(ioBase === oldObjDeserialized)
        assert(ioBase.addendum === oldObjDeserialized.addendum)
      case None =>
        println("You should include the following json as the previousJson argument to test stability across releases.")
        println(json)
    }
    newObj.asInstanceOf[T]
  }

}
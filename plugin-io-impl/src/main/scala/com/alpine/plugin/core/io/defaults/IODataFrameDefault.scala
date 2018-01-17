/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{IODataFrame, TabularSchema}

/**
  * Created by Jennifer Thompson on 9/15/17.
  */
case class IODataFrameDefault(tabularSchema: TabularSchema, addendum: Map[String, AnyRef]) extends IODataFrame

object IODataFrameDefault {

  def apply(tabularSchema: TabularSchema): IODataFrameDefault = {
    IODataFrameDefault(tabularSchema, Map[String, AnyRef]())
  }

}

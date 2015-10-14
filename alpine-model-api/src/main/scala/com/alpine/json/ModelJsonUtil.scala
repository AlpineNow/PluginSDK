/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import java.lang.reflect.Type

import com.alpine.common.serialization.json._
import com.alpine.model._
import com.google.gson.GsonBuilder

/**
 * Utility for Gson augmented with type adapters and known types shorthand.
 */
object ModelJsonUtil {

  val prettyGson = compactGsonBuilder.setPrettyPrinting().create()

  val compactGson = compactGsonBuilder.create()

  def compactGsonBuilder: GsonBuilder = {
    JsonUtil.gsonBuilderWithInterfaceAdapters(typesNeedingInterfaceAdapters)
  }

  def typesNeedingInterfaceAdapters: Seq[Type] = Seq[Type](
    classOf[RowModel],
    classOf[CategoricalRowModel],
    classOf[ClassificationRowModel],
    classOf[ClusteringRowModel],
    classOf[RegressionRowModel]
  )

}

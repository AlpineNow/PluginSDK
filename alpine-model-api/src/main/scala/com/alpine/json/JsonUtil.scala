/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.json

import com.alpine.features._
import com.alpine.model._
import com.google.gson.GsonBuilder

/**
 * Utility for Gson augmented with type adapters and known types shorthand.
 */
object JsonUtil {
  val defaultKnownTypes = KnownTypes(
  Seq[Class[_]](
    classOf[StringType],
    classOf[LongType],
    classOf[IntType],
    classOf[BooleanType],
    classOf[SparseType],
    classOf[DoubleType]
  ).map(c => (c, c.getSimpleName)).toMap)

  val typeKey: String = "type"
  val dataKey: String = "data"

  val prettyGson = compactGsonBuilder.setPrettyPrinting().create()
  val prettyGsonWithTypeHints = compactGsonBuilderWithTypeHints(defaultKnownTypes).setPrettyPrinting().create()

  val compactGson = compactGsonBuilder.create()
  val compactGsonWithTypeHints = compactGsonBuilderWithTypeHints(defaultKnownTypes).create()

  def compactGsonBuilder: GsonBuilder = {
    compactGsonBuilderWithTypeHints(EmptyTypeHints())
  }

  def compactGsonBuilderWithTypeHints(typeHints: TypeHints): GsonBuilder = {
    new GsonBuilder().serializeSpecialFloatingPointValues
      .registerTypeAdapter(classOf[RowModel], new GsonInterfaceAdapter[RowModel])
      .registerTypeAdapter(classOf[CategoricalRowModel], new GsonInterfaceAdapter[CategoricalRowModel])
      .registerTypeAdapter(classOf[ClassificationRowModel], new GsonInterfaceAdapter[ClassificationRowModel])
      .registerTypeAdapter(classOf[RegressionRowModel], new GsonInterfaceAdapter[RegressionRowModel])
      .registerTypeAdapter(classOf[DataType[_]], new GsonInterfaceAdapter[DataType[_]](typeHints))
      .registerTypeHierarchyAdapter(classOf[TypeWrapper[_]], new GsonTypeAdapter(typeHints))
      .registerTypeHierarchyAdapter(classOf[Seq[_]], new GsonSeqAdapter())
      .registerTypeHierarchyAdapter(classOf[Map[_,_]], new GsonMapAdapter())
      .registerTypeHierarchyAdapter(classOf[Option[_]], new GsonOptionAdapter())
  }

  def prettyGsonWithTypeHints(typeHints: TypeHints) = {
    compactGsonBuilderWithTypeHints(typeHints).setPrettyPrinting().create()
  }

  def compactGsonWithTypeHints(typeHints: TypeHints) = {
    compactGsonBuilderWithTypeHints(typeHints).create()
  }

}

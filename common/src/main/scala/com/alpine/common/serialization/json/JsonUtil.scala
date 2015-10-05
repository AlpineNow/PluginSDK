package com.alpine.common.serialization.json

import com.alpine.plugin.core.io.ColumnDef
import com.google.gson.GsonBuilder

/**
 * Utility for Json used to serialize / deserialize IOBase and MLModel types.
 */
object JsonUtil {

  val typeKey: String = "type"
  val dataKey: String = "data"

  def simpleGsonBuilder = {
    new GsonBuilder()
      .serializeSpecialFloatingPointValues
      .addDeserializationExclusionStrategy(new SuperClassExclusionStrategy)
      .addSerializationExclusionStrategy(new SuperClassExclusionStrategy())
      .registerTypeHierarchyAdapter(classOf[Seq[_]], new GsonSeqAdapter())
      .registerTypeHierarchyAdapter(classOf[Map[_,_]], new GsonMapAdapter())
      .registerTypeHierarchyAdapter(classOf[Option[_]], new GsonOptionAdapter())
      .registerTypeHierarchyAdapter(classOf[TypeWrapper[_]], new GsonTypeAdapter())
      .registerTypeAdapter(classOf[ColumnDef], new ColumnDefTypeAdapter)
  }

  def gsonBuilderWithInterfaceAdapters(classes: Seq[java.lang.reflect.Type], classLoaderUtil: Option[ClassLoaderUtil] = None): GsonBuilder = {
    addInterfaceAdaptersToBuilder(classes, simpleGsonBuilder, classLoaderUtil)
  }

  def gsonBuilderWithInterfaceAdapters(classes: Seq[java.lang.reflect.Type], classLoaderUtil: ClassLoaderUtil): GsonBuilder = {
    addInterfaceAdaptersToBuilder(classes, simpleGsonBuilder, Some(classLoaderUtil))
  }

  private def addInterfaceAdaptersToBuilder(classes: Seq[java.lang.reflect.Type], builder: GsonBuilder, classLoaderUtil: Option[ClassLoaderUtil]): GsonBuilder = {
    if (classes.isEmpty) {
      builder
    } else {
      addInterfaceAdaptersToBuilder(classes.tail,
        builder.registerTypeAdapter(
          classes.head,
          new InterfaceAdapter[AnyRef](classLoaderUtil) // Generic type does not actually need to be correct, due to type erasure.
        ),
        classLoaderUtil
      )
    }
  }

}

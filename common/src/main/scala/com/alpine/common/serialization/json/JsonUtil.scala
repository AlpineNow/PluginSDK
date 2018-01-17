package com.alpine.common.serialization.json

import java.io.File
import java.lang.reflect.Type
import java.nio.charset.Charset
import java.nio.file.Files

import com.alpine.plugin.core.io.ColumnDef
import com.google.gson.stream.{JsonReader, JsonWriter}
import com.google.gson.{Gson, GsonBuilder}

/**
  * Utility for Json used to serialize / deserialize IOBase and MLModel types.
  */
object JsonUtil {

  val typeKey: String = "type"
  val dataKey: String = "data"
  val charset: Charset = Charset.forName("UTF-8")

  def simpleGsonBuilder(classLoaderUtil: Option[ClassLoaderUtil] = None): GsonBuilder = {
    new GsonBuilder()
      .serializeSpecialFloatingPointValues
      .addDeserializationExclusionStrategy(new SuperClassExclusionStrategy)
      .addSerializationExclusionStrategy(new SuperClassExclusionStrategy())
      .registerTypeHierarchyAdapter(classOf[Seq[_]], GsonSeqAdapter())
      .registerTypeHierarchyAdapter(classOf[Set[_]], GsonSetAdapter())
      .registerTypeHierarchyAdapter(classOf[Map[_, _]], new GsonMapAdapter())
      .registerTypeHierarchyAdapter(classOf[Option[_]], GsonOptionAdapter())
      .registerTypeHierarchyAdapter(classOf[TypeWrapper[_]], new GsonTypeAdapter(classLoaderUtil))
      .registerTypeAdapter(classOf[ColumnDef], new ColumnDefTypeAdapter)
  }

  def gsonBuilderWithInterfaceAdapters(classes: Seq[java.lang.reflect.Type], classLoaderUtil: Option[ClassLoaderUtil] = None): GsonBuilder = {
    addInterfaceAdaptersToBuilder(classes, simpleGsonBuilder(classLoaderUtil), classLoaderUtil)
  }

  def gsonBuilderWithInterfaceAdapters(classes: Seq[java.lang.reflect.Type], classLoaderUtil: ClassLoaderUtil): GsonBuilder = {
    addInterfaceAdaptersToBuilder(classes, simpleGsonBuilder(Some(classLoaderUtil)), Some(classLoaderUtil))
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

  def writeToFile(gson: Gson)(obj: Any, `type`: Type, file: File, writePrettily: Boolean): Unit = {
    val fileWriter = Files.newBufferedWriter(file.toPath, charset)
    try {
      val jsonWriter = new JsonWriter(fileWriter)
      if (writePrettily) {
        jsonWriter.setIndent("  ")
      }
      try {
        gson.toJson(obj, `type`, jsonWriter)
      } finally {
        jsonWriter.flush()
        jsonWriter.close()
      }
    } finally {
      fileWriter.close()
    }
  }

  def readFromFile[T](gson: Gson)(file: File, `type`: Type): T = {
    val fileReader = Files.newBufferedReader(file.toPath, charset)
    try {
      gson.fromJson(new JsonReader(fileReader), `type`)
    } finally {
      fileReader.close()
    }
  }

}

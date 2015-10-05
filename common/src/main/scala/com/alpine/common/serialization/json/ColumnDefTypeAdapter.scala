package com.alpine.common.serialization.json

import com.alpine.plugin.core.io.ColumnDef
import com.alpine.plugin.core.io.ColumnType
import com.google.gson._
import java.lang.reflect.Type

class ColumnDefTypeAdapter extends JsonSerializer[ColumnDef] with JsonDeserializer[ColumnDef] {

  private val columnNameKey: String = "columnName"
  private val columnTypeKey: String = "columnType"

  def serialize(columnDef: ColumnDef, `type`: Type, context: JsonSerializationContext): JsonElement = {
    val result: JsonObject = new JsonObject
    result.add(columnNameKey, new JsonPrimitive(columnDef.columnName))
    result.add(columnTypeKey, new JsonPrimitive(columnDef.columnType.name))
    result
  }

  @throws(classOf[JsonParseException])
  def deserialize(elem: JsonElement, `type`: Type, context: JsonDeserializationContext): ColumnDef = {
    val jsonObj: JsonObject = elem.asInstanceOf[JsonObject]
    val columnName: JsonElement = jsonObj.get(columnNameKey)
    val columnType: JsonElement = jsonObj.get(columnTypeKey)
    new ColumnDef(columnName.getAsString, new ColumnType.TypeValue(columnType.getAsString))
  }
}
package com.alpine.common.serialization.json

import com.alpine.plugin.core.io.ColumnDef
import com.alpine.plugin.core.io.ColumnType
import com.google.gson._
import java.lang.reflect.Type

class ColumnDefTypeAdapter extends JsonSerializer[ColumnDef] with JsonDeserializer[ColumnDef] {

  private val columnNameKey: String = "columnName"
  private val columnTypeKey: String = "columnType"
  private val columnTypeFormatKey: String = "columnTypeFormat"

  def serialize(columnDef: ColumnDef, `type`: Type, context: JsonSerializationContext): JsonElement = {
    val result: JsonObject = new JsonObject
    result.add(columnNameKey, new JsonPrimitive(columnDef.columnName))
    result.add(columnTypeKey, new JsonPrimitive(columnDef.columnType.name))
    if (columnDef.columnType.format.isDefined) {
      result.add(columnTypeFormatKey, new JsonPrimitive(columnDef.columnType.format.get))
    }
    result
  }

  @throws(classOf[JsonParseException])
  def deserialize(elem: JsonElement, `type`: Type, context: JsonDeserializationContext): ColumnDef = {
    val jsonObj: JsonObject = elem.asInstanceOf[JsonObject]
    val columnName: JsonElement = jsonObj.get(columnNameKey)
    val columnType: JsonElement = jsonObj.get(columnTypeKey)
    val columnTypeFormat: JsonElement = jsonObj.get(columnTypeFormatKey)
    val format = Option.apply(columnTypeFormat).map(_.getAsString)
    new ColumnDef(columnName.getAsString, new ColumnType.TypeValue(columnType.getAsString, format))
  }
}
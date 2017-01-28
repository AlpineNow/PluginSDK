/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.avrotypes

import com.alpine.model.export.pfa.expressions.PFAExpression
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}

/**
  * Could add fixed, enum.
  *
  * Created by Jennifer Thompson on 5/27/16.
  */
object AvroTypes {

  val double = PrimitiveType("double")
  val int = PrimitiveType("int")
  val long = PrimitiveType("long")
  val string = PrimitiveType("string")
  val nullType = PrimitiveType("null")
  val boolean = PrimitiveType("boolean")
  val float = PrimitiveType("float")
  val bytes = PrimitiveType("bytes")

  val arrayDouble = ArrayType(double)
  val mapDouble = MapType(double)
  val arrayInt = ArrayType(int)
  val arrayString = ArrayType(string)

  def fromAlpineSchema(name: String, schema: Seq[ColumnDef], allowNullValues: Boolean = false): RecordType = {
    RecordType(name, schema.map(fromColumnDef(_, allowNullValues)))
  }

  def fromAlpineType(t: ColumnType.TypeValue): AvroType = {
    t match {
      case ColumnType.String => string
      case ColumnType.Int => int
      case ColumnType.Long => long
      case ColumnType.Double => double
      case ColumnType.Float => float
      case ColumnType.Sparse => MapType(double)
      case ColumnType.DateTime => ???
      case _ => ???
    }
  }

  def fromColumnDef(c: ColumnDef, allowNullValues: Boolean): FieldType = {
    val avroType: AvroType = {
      if (allowNullValues) {
        UnionType(fromAlpineType(c.columnType), nullType)
      } else {
        fromAlpineType(c.columnType)
      }
    }
    FieldType(c.columnName, avroType)
  }

}

trait AvroType

/**
  * Usually refers to the name of a record type declared elsewhere (e.g. the output type).
  *
  * Will be serialized just as the content of the raw val, not as {raw: ...}.
  *
  * @param raw The name of the type.
  */
case class AvroTypeReference(raw: String) extends PFAExpression with AvroType

case class PrimitiveType(raw: String) extends PFAExpression with AvroType

case class ArrayType(items: AvroType) extends AvroType {
  val `type` = "array"
}

// Keys must be Strings.
case class MapType(values: AvroType) extends AvroType {
  val `type` = "map"
}

case class RecordType(name: String, fields: Seq[FieldType]) extends AvroType {
  val `type` = "record"

  def withNewName(newName: String) = RecordType(newName, fields)
}

case class UnionType(types: Seq[AvroType]) extends AvroType with PFAExpression {
  /**
    * This is the value that will be JSONised.
    */
  override def raw: Any = types
}

object UnionType {
  def apply(firstType: AvroType, secondType: AvroType): UnionType = UnionType(Seq(firstType, secondType))
}

case class FieldType(name: String, `type`: AvroType)

// TODO: Enum, Fixed.

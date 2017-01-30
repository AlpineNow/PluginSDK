/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.utils

/** *
  *
  * @deprecated
  * Use HdfsStorageFormatTypes which are case classes inheriting from the sealed trait HdfsStorageFormatType instead.
  */
@deprecated("Use HdfsStorageFormatType case class")
object HdfsStorageFormat extends Enumeration {
  type HdfsStorageFormat = Value
  val Parquet, Avro, TSV = Value
}

sealed trait HdfsStorageFormatType {

}


case class Parquet() extends HdfsStorageFormatType {
  override def toString: String = "Parquet"
}

case class Avro() extends HdfsStorageFormatType {
  override def toString: String = "Avro"
}

case class CSV() extends HdfsStorageFormatType {
  override def toString: String = "CSV"
}

@deprecated("Use CSV")
case class TSV() extends HdfsStorageFormatType {
  override def toString: String = "CSV"
}


object HdfsStorageFormatType {
  val Parquet = new Parquet()
  val Avro = new Avro()
  val CSV = new CSV()
  val TSV = new TSV()


  val values = Seq(Parquet, Avro, CSV)

  def withName(s: String): HdfsStorageFormatType = {
    if (s.equals("Parquet"))
      Parquet
    else if (s.equals("Avro"))
      Avro
    else if (s.equals("CSV"))
      CSV
    else if (s.equals("TSV"))
      CSV
    else
      throw new MatchError("The type " + s + "is not a valid HdfsStorageFormatType")
  }
}
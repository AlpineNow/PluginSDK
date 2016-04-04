/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.utils

import com.alpine.plugin.core.utils.{TSV, Avro, Parquet}

sealed trait HdfsStorageFormat {

}

case class Parquet() extends HdfsStorageFormat {
  override def toString: String = "Parquet"
}

case class Avro() extends HdfsStorageFormat {
  override def toString: String = "Avro"
}

case class TSV() extends HdfsStorageFormat {
  override def toString: String = "TSV"
}

object HdfsStorageFormat {
  val Parquet = new Parquet()
  val Avro = new Avro()
  val TSV = new TSV()

  val values = Seq(Parquet, Avro, TSV)

  def withName(s: String): HdfsStorageFormat = {
    if (s.equals("Parquet"))
      Parquet
    else if (s.equals("Avro"))
      Avro
    else if (s.equals("TSV"))
      TSV
    else
      throw new MatchError("The type " + s + "is not a valid HdfsStorageFormatType")
  }
}
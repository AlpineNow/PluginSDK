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

  /***
    * Checks if the compression type is supported by the current storage format
    * @param compressionType HdfsCompressionType for which we want to check compatibility
    * @return true if the compression type is supported
    */
  def isCompatibleWithCompression(compressionType: HdfsCompressionType) = {
    this match {
      case HdfsStorageFormatType.CSV => HdfsCompressionType.csvSupportedValues.contains(compressionType)
      case HdfsStorageFormatType.Parquet => HdfsCompressionType.parquetSupportedValues.contains(compressionType)
      case HdfsStorageFormatType.Avro => HdfsCompressionType.avroSupportedValues.contains(compressionType)
      case _ => true
    }
  }

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

  @deprecated(" use CSV")
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


//For now only used for Parquet
sealed trait HdfsCompressionType {
  def toSparkName: String
}


object HdfsCompressionType {

  val COMPRESSION_OPTION_PARAM = "compression"
  val AVRO_COMPRESSION_SPARK_PARAM = "spark.sql.avro.compression.codec"

  val GZIP_UI_NAME = "GZIP"
  val DEFLATE_UI_NAME = "Deflate"
  val SNAPPY_UI_NAME = "Snappy"
  val NO_COMPRESSION_UI_NAME = "No Compression"

  val Snappy = new Snappy
  val Gzip = new Gzip
  val NoCompression = new NoCompression
  val Deflate = new Deflate

  val values = Seq(NoCompression, Deflate, Snappy, Gzip)

  val parquetSupportedValues = Seq(Snappy, Gzip, NoCompression)
  val csvSupportedValues = Seq(Gzip, NoCompression, Deflate) // removing Snappy support for CSV as we couldn't make it work
  val avroSupportedValues = Seq(Snappy, Deflate, NoCompression)

  def withName(s: String): HdfsCompressionType = {
    s match {
      case GZIP_UI_NAME => Gzip
      case SNAPPY_UI_NAME => Snappy
      case DEFLATE_UI_NAME => Deflate
      case NO_COMPRESSION_UI_NAME => NoCompression
      case _ => throw new MatchError("The compression type " + s + "is not a valid HdfsCompressionType.")
    }
  }
}

//toSparkName returns the exact strings xxx to pass as options in df.write.option("compression",xxx)) (which override default value of spark.sql.parquet.compression.codec)
case class NoCompression() extends HdfsCompressionType {
  override def toSparkName: String = "none"
  override def toString : String = HdfsCompressionType.NO_COMPRESSION_UI_NAME
}

case class Snappy() extends HdfsCompressionType {
  override def toSparkName: String = "snappy"
  override def toString : String = HdfsCompressionType.SNAPPY_UI_NAME
}

case class Gzip() extends HdfsCompressionType {
  override def toSparkName: String = "gzip"
  override def toString : String = HdfsCompressionType.GZIP_UI_NAME
}

case class Deflate() extends HdfsCompressionType {
  override def toSparkName: String = "deflate"
  override def toString : String = HdfsCompressionType.DEFLATE_UI_NAME
}



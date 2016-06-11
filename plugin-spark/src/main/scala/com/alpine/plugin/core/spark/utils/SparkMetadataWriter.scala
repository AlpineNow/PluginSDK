package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.io.{ColumnType, IOList, HdfsDelimitedTabularDataset, IOBase}
import com.google.gson.{FieldNamingPolicy, GsonBuilder, Gson}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.JavaConverters._
/**
  * Created by rachelwarren on 5/23/16.
  */

object HadoopDataType {
  val INT = "int"
  val LONG = "long"
  val FLOAT = "float"
  val DOUBLE = "double"
  val CHARARRAY = "chararray"
  val DATETIME = "datetime"
  val BOOLEAN = "boolean"
  val SPARSE = "sparse"
}

object SparkMetadataWriter {
  val METADATA_FILENAME = ".alpine_metadata"

  private val gson: Gson = new GsonBuilder()
    // I don't know why we change the field names, but it's legacy behaviour.
    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
    .create

  def writeMetadataForDelimitedDatasets(ioBase: IOBase, hdfs: FileSystem): Array[Path] = {
    try {
      ioBase match {
        case dataset: HdfsDelimitedTabularDataset => {
         val p = writeMetadataForDataset(dataset, hdfs)
         p match {
           case Some(p) => Array(p)
           case None => Array()
         }
        }
        case list: IOList[_] => {
         list.elements.flatMap(element =>
           writeMetadataForDelimitedDatasets(element, hdfs)).toArray
        }
        case tuple: com.alpine.plugin.core.io.Tuple => {
          tuple.elements.flatMap(element =>
            writeMetadataForDelimitedDatasets(element, hdfs)).toArray
        }
        case _ => println("warning: can not write metadata for IOBase Type " + ioBase.displayName)
          Array()
      }
    }
    catch {
      case e: Exception => {
        println("Non-fatal error occurred when writing .alpine_metadata file(s):")
        Array()
        // Do nothing, as the output may still be usable.
      }
    }
  }

  /**
    * Returns if the metadata can be written returns Some(Path) where Path is the Path option representing the
    * location
    * of the metada. None otherwise.
    */
  def writeMetadataForDataset(dataset: HdfsDelimitedTabularDataset, hdfs: FileSystem): Option[Path]  = {
    val datasetPath = new Path(dataset.path)
    if (hdfs.isDirectory(datasetPath)) {
      val metadata: HadoopMetadata = convertToMetadataObject(dataset)
      val metadataJSON = gson.toJson(metadata)

      val metadataPath = new Path(datasetPath, METADATA_FILENAME)
      if (!hdfs.exists(metadataPath)) {
        writeSmallTextFile(hdfs, metadataPath, metadataJSON)
       Some(metadataPath)
      } else {
        // Maybe the user is passing along an input object as output, where the folder already has metadata.
        None
      }
    } else {
      // If it's not a directory, then we can't write the metadata inside the directory.
      None
    }
  }


  def convertToMetadataObject(dataset: HdfsDelimitedTabularDataset): HadoopMetadata = {
    val fixedColumns = dataset.tabularSchema.getDefinedColumns
    val columnNames = fixedColumns.map(c => c.columnName)
    val columnTypes= fixedColumns.map(c => convertToAlpineType(c.columnType))

     new HadoopMetadata(columnNames.asJava,
       columnTypes = columnTypes.asJava,
       delimiter = dataset.tsvAttributes.delimiter.toString,
       quote = dataset.tsvAttributes.quoteStr.toString,
       escape = dataset.tsvAttributes.escapeStr.toString,
       isFirstLineHeader = dataset.tsvAttributes.containsHeader)
  }

  def convertToAlpineType(colType: ColumnType.TypeValue): String = {
    if (colType == ColumnType.Int) {
       HadoopDataType.INT
    }
    else if (colType == ColumnType.Long) {
       HadoopDataType.LONG
    }
    else if (colType == ColumnType.Float) {
       HadoopDataType.FLOAT
    }
    else if (colType == ColumnType.Double) {
       HadoopDataType.DOUBLE
    }
    else if (colType == ColumnType.String) {
       HadoopDataType.CHARARRAY
    }
    else if (colType == ColumnType.DateTime) {
      HadoopDataType.DATETIME
    }
    else if (colType == ColumnType.Sparse) {
       HadoopDataType.SPARSE
    }
    else {
      HadoopDataType.CHARARRAY
    }
  }

  def writeSmallTextFile(hdfs: FileSystem, outputPath: Path, fileContent: String): Unit = {
    val outputStream = hdfs.create(outputPath, true)
    // Change the permission flags of the output file.
    hdfs.setPermission(
      outputPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
    )

    outputStream.write(fileContent.getBytes)
    outputStream.flush()
    outputStream.close()
  }
}

case class HadoopMetadata(
   columnNames: java.util.List[String],
   columnTypes: java.util.List[String],
   delimiter: String,
   escape: String,
   quote: String,
   isFirstLineHeader: Boolean = false,
   totalNumberOfRows: Long = -1){
}


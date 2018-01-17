package com.alpine.plugin.core.spark.utils


import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.zip.{GZIPInputStream, InflaterInputStream}

import au.com.bytecode.opencsv.CSVParser
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.spark.{OperatorFailedException, SparkExecutionContext}
import com.alpine.plugin.util.JavaMemoryUsage
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.schema.MessageType
import org.apache.parquet.example.data.Group

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by emiliedelongueau on 7/26/17.
  */



object HDFSFileReaderUtils {
  val bytesInMB = 1048576.0
  val co_config_max_file_size_MB_key = "max_mb_input_file_size_alpine_server"

}

/** *
  * Class for reading hdfs files locally on the Alpine Server.
  * Use either:
  * - the general method `openFileGeneralAndProcessLines` that processes rows of data in a single part file of an HdfsTabularDataset input (of any storage format)
  * - or the relevant method for reading a single hdfs file depending on the file storage format (CSV, Avro, Parquet). It supports compressed files.
  * It allows setting a max size limit for the input file (disk space usage w/o cluster replication) to avoid memory issues if the file is very large.
  * (default is 1000MB).
  *
  * For each storage format, there is
  * - a generic method which allows to pass as argument a function applied to the file reader
  * (e.g openCSVFileAndDoAction expecting function argument: action: InputStreamReader => Unit)
  * - a method which allows to loop through the rows of data in the file, which requires as argument a function to apply on an Iterator[RowWithIndex]
  * (e.g openCSVFileAndProcessLines expecting function argument resultHandler: Iterator[RowWithIndex] => Unit)
  * The second method checks for java memory usage while processing the lines, and stops the process if the memory usage is over the limit (default 90)%.
  */
class HDFSFileReaderUtils(context: SparkExecutionContext) {

  val defaultFileSizeLimitMB = 500.0

  lazy val defaultFileSizeLimitMBFromConfig =
    Try(context.config.entries(HDFSFileReaderUtils.co_config_max_file_size_MB_key).toString.toDouble).getOrElse(defaultFileSizeLimitMB)


  /** *
    * Method to process a single part file in the directory path of an HdfsTabularDataset, by applying a user-specified function on the row iterator
    * of the part file.
    * Compression types supported depend on the storage format (see methods below specific to each storage format).
    *
    * @param partFilePath          Path of the part file to process.
    * @param parentDataset         Parent HdfsTabularDataset that contains the part file in its directory path.
    * @param partFileResultHandler function(Iterator[RowWithIndex] => Unit) to apply on the row iterator of the part file, which defines
    *                              how to process it. Note: the rowNum parameter of RowWithIndex starts at 0 for the first iterator value.
    * @param fileSizeLimitMB       Optional file size limit checked before opening each part file (disk space usage w/o replication)
    */
  def openFileGeneralAndProcessLines(
      partFilePath: Path,
      parentDataset: HdfsTabularDataset,
      partFileResultHandler: Iterator[RowWithIndex] => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    parentDataset match {
      case d: HdfsDelimitedTabularDataset =>
        openCSVFileAndProcessLines(partFilePath, d.tsvAttributes, partFileResultHandler, fileSizeLimitMB)
      case _: HdfsParquetDataset =>
        openParquetFileAndProcessLines(partFilePath, partFileResultHandler, fileSizeLimitMB)
      case _: HdfsAvroDataset =>
        openAvroFileAndProcessLines(partFilePath, partFileResultHandler, fileSizeLimitMB)
      case _ => throw new OperatorFailedException("Storage format of input dataset is not supported.")
    }
  }

  // ======================================================================
  // For CSV storage format.
  // ======================================================================
  /**
    * Generic method to process a single HDFS file in CSV storage format.
    * Compression types supported: No Compression, Deflate, GZIP.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param action          function(InputStreamReader => Unit) to apply on the InputStreamReader, which defines how to process the file.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    */
  @throws(classOf[FileTooLargeException])
  def openCSVFileAndDoAction(
      hdfsPath: Path,
      action: BufferedReader => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    val fileStatus = context.doHdfsAction(fs => fs.getFileStatus(hdfsPath))
    val inputPath = fileStatus.getPath
    if (fileStatus.isDirectory) {
      throw new UnsupportedOperationException(s"Cannot open file $hdfsPath as it is a directory.")
    }
    if (fileSizeLimitMB.isDefined) checkFileSizeUnderLimit(hdfsPath, fileSizeLimitMB.get)

    try {
      context.doHdfsAction { fs =>
        val fis = if (HdfsCSVFileCompressUtils.isCompressed(inputPath))
          HdfsCSVFileCompressUtils.decompressInputStream(fileStatus.getPath, fs.open(inputPath)) else fs.open(inputPath)
        val ir: InputStreamReader = new InputStreamReader(fis)
        val br = new BufferedReader(ir)
        try {
          //apply user's specified action
          action(br)
        }
        finally {
          fis.close()
          ir.close()
          br.close()
        }
      }
    }
    catch {
      case e: Exception => throw new Exception(s"An error occurred when trying to process file $hdfsPath: " + e.getMessage)
    }
  }


  /** *
    * Method to process single HDFS file in CSV storage format by looping through the lines of data
    * and applying a user-specified function on each row.
    * Compression types supported: No Compression, Deflate, GZIP.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param tSVAttributes   TSVAttributes of the delimited input.
    * @param resultHandler   function(Iterator[RowWithIndex] => Unit) to apply on the row iterator of the part file, which defines
    *                        how to process it. Note: the rowNum parameter of RowWithIndex starts at 0 for the first iterator value.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    *
    */
  def openCSVFileAndProcessLines(
      hdfsPath: Path,
      tSVAttributes: TSVAttributes,
      resultHandler: Iterator[RowWithIndex] => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    val lineParser: CSVParser = new CSVParser(tSVAttributes.delimiter, tSVAttributes.quoteStr, tSVAttributes.escapeStr)
    openCSVFileAndDoAction(hdfsPath, action = processCSVFileLines(lineParser, resultHandler), fileSizeLimitMB)

    def processCSVFileLines(lineParser: CSVParser, resultHandler: Iterator[RowWithIndex] => Unit)(br: BufferedReader): Unit = {
      val javaMemoryUsage = new JavaMemoryUsage(context.config)
      val lineIterator = createCSVFileRowIterator(lineParser, br, javaMemoryUsage)
      resultHandler(lineIterator)
    }
  }

  // ======================================================================
  // For PARQUET storage format.
  // ======================================================================
  /**
    * Generic method to process a single HDFS file in Parquet storage format.
    * Compression types supported: No Compression, GZIP, Snappy.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param action          function((reader: ParquetReader[Group], schema: MessageType) => Unit) to apply, which defines how to process the file.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    */
  @throws(classOf[FileTooLargeException])
  def openParquetFileAndDoAction(
      hdfsPath: Path,
      action: ((ParquetReader[Group], MessageType) => Unit),
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    val fileStatus = context.doHdfsAction(fs => fs.getFileStatus(hdfsPath))

    if (fileStatus.isDirectory) {
      throw new UnsupportedOperationException(s"Cannot open file $hdfsPath as it is a directory.")
    }
    if (fileSizeLimitMB.isDefined) checkFileSizeUnderLimit(hdfsPath, fileSizeLimitMB.get)

    try {
      context.doHdfsAction { fs =>
        val readFooter: ParquetMetadata = ParquetFileReader.readFooter(fs.getConf, fileStatus.getPath, ParquetMetadataConverter.NO_FILTER)
        val schema: MessageType = readFooter.getFileMetaData.getSchema
        val readSupport: GroupReadSupport = new GroupReadSupport
        readSupport.init(fs.getConf, null, schema)

        val reader = new ParquetReader(fileStatus.getPath, readSupport)
        try {
          //apply user's specified action
          action(reader, schema)
        }
        finally {
          Try(reader.close()) //quietly ignore if exception
        }
      }
    }
    catch {
      case e: Exception => throw new Exception(s"An error occurred when trying to process Parquet file $hdfsPath: " + e.getMessage)
    }
  }

  /** *
    * Method to process single HDFS file in Parquet storage format by looping through the lines of data
    * and applying a user-specified function on each row.
    * Compression types supported: No Compression, GZIP, Snappy.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param resultHandler   function(Iterator[RowWithIndex] => Unit) to apply on the row iterator of the part file, which defines
    *                        how to process it. Note: the rowNum parameter of RowWithIndex starts at 0 for the first iterator value.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    *
    */
  def openParquetFileAndProcessLines(
      hdfsPath: Path,
      resultHandler: Iterator[RowWithIndex] => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    openParquetFileAndDoAction(hdfsPath, action = processParquetFileLines(resultHandler), fileSizeLimitMB)

    def processParquetFileLines(resultHandler: Iterator[RowWithIndex] => Unit)(parquetReader: ParquetReader[Group], schema: MessageType): Unit = {
      val javaMemoryUsage = new JavaMemoryUsage(context.config)
      val lineIterator = createParquetFileRowIterator(parquetReader, schema, javaMemoryUsage)
      resultHandler(lineIterator)
    }
  }

  // ======================================================================
  // For Avro storage format.
  // ======================================================================
  /**
    * Generic method to process a single HDFS file in Avro storage format.
    * Compression types supported: No Compression, Deflate, Snappy.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param action          function((reader: FileReader[GenericRecord], schema: Schema) => Unit) to apply, which defines how to process the file.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    */
  @throws(classOf[FileTooLargeException])
  def openAvroFileAndDoAction(
      hdfsPath: Path,
      action: (FileReader[GenericRecord], Schema) => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    val fileStatus = context.doHdfsAction(fs => fs.getFileStatus(hdfsPath))
    if (fileStatus.isDirectory) {
      throw new UnsupportedOperationException(s"Cannot open file $hdfsPath as it is a directory.")
    }
    if (fileSizeLimitMB.isDefined) checkFileSizeUnderLimit(hdfsPath, fileSizeLimitMB.get)

    try {
      context.doHdfsAction { fs =>
        val avroReader: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]
        val fileReader: FileReader[GenericRecord] = DataFileReader.openReader(new FsInput(fileStatus.getPath, fs.getConf), avroReader)
        val schema: Schema = fileReader.getSchema

        try {
          //apply user's specified action
          action(fileReader, schema)
        }
        finally {
          Try(fileReader.close()) //quietly ignore if exception
        }
      }
    }
    catch {
      case e: Exception => throw new Exception(s"An error occurred when trying to process Avro file $hdfsPath: " + e.getMessage)
    }
  }

  /** *
    * Method to process single HDFS file in Avro storage format by looping through the lines of data
    * and applying a user-specified function on each row.
    * Compression types supported: No Compression, Deflate, Snappy.
    *
    * @param hdfsPath        hdfs path of the input file
    * @param resultHandler   function(Iterator[RowWithIndex] => Unit) to apply on the row iterator of the part file, which defines
    *                        how to process it. Note: the rowNum parameter of RowWithIndex starts at 0 for the first iterator value.
    * @param fileSizeLimitMB Optional file size limit (disk space usage w/o replication)
    *
    */
  def openAvroFileAndProcessLines(
      hdfsPath: Path,
      resultHandler: Iterator[RowWithIndex] => Unit,
      fileSizeLimitMB: Option[Double] = Some(defaultFileSizeLimitMBFromConfig)): Unit = {

    openAvroFileAndDoAction(hdfsPath, action = processAvroLines(resultHandler), fileSizeLimitMB)

    def processAvroLines(resultHandler: Iterator[RowWithIndex] => Unit)(avroFileReader: FileReader[GenericRecord], schema: Schema): Unit = {
      val javaMemoryUsage = new JavaMemoryUsage(context.config)
      val lineIterator: Iterator[RowWithIndex] = createAvroFileRowIterator(avroFileReader, schema, javaMemoryUsage)
      resultHandler(lineIterator)
    }
  }


  // ======================================================================
  // Helper methods
  // ======================================================================

  private def createCSVFileRowIterator(
      lineParser: CSVParser,
      br: BufferedReader,
      javaMemoryUsage: JavaMemoryUsage) = {

    Iterator.continually(br.readLine()).takeWhile(_ != null).zipWithIndex.map { case (row: String, rowNum: Int) =>
      checkJavaMemoryUsage(javaMemoryUsage, rowNum)
      RowWithIndex(lineParser.parseLine(row), rowNum)
    }
  }

  private def createParquetFileRowIterator(parquetReader: ParquetReader[Group], schema: MessageType, javaMemoryUsage: JavaMemoryUsage) = {
    val numFields = schema.getFieldCount
    val columnRange = 0 until numFields

    Iterator.continually(parquetReader.read()).takeWhile(_ != null).zipWithIndex.map { case (g: Group, rowNum: Int) =>
      checkJavaMemoryUsage(javaMemoryUsage, rowNum)
      val buffer = new ArrayBuffer[String]()
      for (i <- columnRange) {
        if (g.getFieldRepetitionCount(i) == 0 || (g.getFieldRepetitionCount(i) != 1 && !schema.getType(i).isPrimitive)) {
          buffer.append("")
        }
        else {
          buffer.append(g.getValueToString(i, 0))
        }
      }
      RowWithIndex(buffer.toArray, rowNum)
    }
  }

  private def createAvroFileRowIterator(avroReader: FileReader[GenericRecord], schema: Schema, javaMemoryUsage: JavaMemoryUsage) = {
    val numFields = schema.getFields.size()
    val columnRange = 0 until numFields
    import scala.collection.JavaConverters._

    avroReader.iterator().asScala.zipWithIndex.map { case (record: GenericRecord, rowNum: Int) =>
      checkJavaMemoryUsage(javaMemoryUsage, rowNum)
      val buffer = new ArrayBuffer[String]()
      for (i <- columnRange) {
        val data = record.get(i)
        if (null == data) buffer.append("")
        else buffer.append(data.toString)
      }
      RowWithIndex(buffer.toArray, rowNum)
    }
  }

  private def checkJavaMemoryUsage(javaMemoryUsage: JavaMemoryUsage, rowNum: Int) = {
    if (rowNum % 5000 == 0 && !javaMemoryUsage.isMemoryUsageUnderLimit) {
      throw new OperatorFailedException(s"There is not enough memory available to process the file: " + javaMemoryUsage.toString)
    }
  }

  //Get the input file disk usage w/o cluster replication factor
  private def getFileDiskUsageMB(hdfsPath: Path) = {
    context.doHdfsAction(_.getContentSummary(hdfsPath).getLength.toDouble) / HDFSFileReaderUtils.bytesInMB
  }

  @throws(classOf[FileTooLargeException])
  private def checkFileSizeUnderLimit(hdfsPath: Path, limitMB: Double): Unit = {
    val fileSizeMB: Double = getFileDiskUsageMB(hdfsPath)
    if (fileSizeMB > limitMB)
      throw new FileTooLargeException(fileSizeMB, limitMB, s"The input file ${hdfsPath.getName} is too large to be processed on the Alpine Server " +
        s"(Disk space usage: ${math.round(100.0 * fileSizeMB) / 100.0} MB, Limit set in operator: ${math.round(100.0 * limitMB) / 100.0} MB). ")
  }

}


/** *
  * Utils to open and uncompress a CSV (or delimited) file as an inputStream.
  * Compressions supported: No Compression, GZIP, Deflate. Snappy compression is not supported.
  */
object HdfsCSVFileCompressUtils {

  @throws(classOf[UnsupportedOperationException])
  def decompressInputStream(path: Path, is: InputStream): InputStream = {
    if (isGzip(path)) new GZIPInputStream(is)
    else if (isDeflate(path)) new InflaterInputStream(is)
    else if (isSnappy(path)) {
      is.close()
      throw new UnsupportedOperationException("Inputs using Snappy compression are not supported.")
    }
    else is
  }

  def isCompressed(path: Path) = {
    isGzip(path) | isDeflate(path) | isSnappy(path)
  }

  def isGzip(path: Path) = {
    val pathString = path.toString
    pathString != null && pathString.trim.length != 0 && pathString.endsWith(".gz")
  }

  def isDeflate(path: Path) = {
    val pathString = path.toString
    pathString != null && pathString.trim.length != 0 && pathString.endsWith(".deflate")
  }

  def isSnappy(path: Path) = {
    val pathString = path.toString
    pathString != null && pathString.trim.length != 0 && pathString.endsWith(".snappy")
  }
}


/** *
  * Utils to retrieve all relevant part files for analysis in an HDFS input directory
  */
object HDFSFileFilter extends PathFilter {

  def accept(path: Path) = {
    val name = path.getName
    !name.startsWith(".") && !name.startsWith("_") && !name.endsWith(".crc")
  }

  /** *
    * Get the sequence of all file paths in the HDFS input directory (or single file), except paths names starting with ".", "_" or ending with ".crc"
    *
    * @param context   SparkExecutionContext
    * @param inputPath file or directory hdfs input path
    */
  @throws(classOf[OperatorFailedException])
  def getAllPartFiles(
      context: SparkExecutionContext,
      inputPath: String): Seq[Path] = {

    context.doHdfsAction { fs =>
      val cleanedPath: String = cleanPath(inputPath)
      val inputStatus = fs.getFileStatus(new Path(cleanedPath))
      if (inputStatus.isFile) Seq(inputStatus.getPath)
      else {
        val filesPaths = fs.listStatus(inputStatus.getPath).map(_.getPath).filter(HDFSFileFilter.accept).toSeq
        if (filesPaths.isEmpty) throw new OperatorFailedException(s"""No valid files were found in path ${inputStatus.getPath}""")
        filesPaths
      }
    }
  }

  def cleanPath(path: String): String = {
    if (path.endsWith("/") || path.endsWith("/*")) {
      path.splitAt(path.lastIndexOf("/"))._1
    }
    else path
  }
}

/** *
  * Class representing a row of data
  *
  * @param values string values of the row of data
  * @param rowNum row number in the part file (starts at 0 for the 1st row)
  */
case class RowWithIndex(values: Array[String], rowNum: Int)

/**
  * Exception thrown if the user is trying to read a file for which the disk space usage on HDFS w/o replication is above the limit configured.
  * If the operator developer is getting the file size limit parameter (fileSizeLimitMB) from the SparkExecutionContext.config, the message should indicate to the end user that this threshold
  * is configurable in the alpine configuration file, in the "custom_operator" section.
  */
final class FileTooLargeException(val actualSizeMB: Double, val fileSizeLimitMB: Double, message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)
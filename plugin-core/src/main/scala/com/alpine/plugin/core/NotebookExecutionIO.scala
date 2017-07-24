package com.alpine.plugin.core

import java.util

import com.alpine.plugin.core.utils.HdfsStorageFormatType

/**
  * Created by emiliedelongueau on 7/4/17.
  */


//classes to create the body of the runNotebookExecution api in ChorusAPICaller (for input(s) / output substitution)

abstract class NotebookIOForExecution {
  val executionLabel: String
  def toBodyMap: java.util.HashMap[String, Any]
}

case class NotebookInputHDForExecution(executionLabel: String, path: String, header: Boolean, delimiter: Option[String], quote: Option[String],
                                       escape: Option[String], columnNames: Seq[String], columnTypes: Seq[String]) extends NotebookIOForExecution {

  def toBodyMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("path", path)
    map.put("header", header)
    if (delimiter.isDefined) map.put("delimiter", delimiter.get)
    if (quote.isDefined) map.put("quote", quote.get)
    if (escape.isDefined) map.put("escape", escape.get)
    map.put("column_names", columnNames.toArray)
    map.put("column_types", columnTypes.toArray)

    map
  }
}

case class NotebookOutputHDForExecution(executionLabel: String, outputPath: String, delimiter: Option[String], quote: Option[String],
                                        escape: Option[String], overwrite: Boolean, storageFormat: Option[String]) extends NotebookIOForExecution {

  def toBodyMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("path", outputPath)
    if (delimiter.isDefined) map.put("delimiter", delimiter.get)
    if (quote.isDefined) map.put("quote", quote.get)
    if (escape.isDefined) map.put("escape", escape.get)
    map.put("storage_format", storageFormat.getOrElse(HdfsStorageFormatType.CSV.toString))
    map.put("overwrite", overwrite)
    map
  }
}

case class NotebookInputDBForExecution(executionLabel: String, tableName: String, schemaName: String, databaseName: String) extends NotebookIOForExecution {

  def toBodyMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("table_name", tableName)
    map.put("schema_name", schemaName)
    map.put("database_name", databaseName)
    map
  }
}

case class NotebookOutputDBForExecution(executionLabel: String, tableName: String, schemaName: String, databaseName: String, overwrite: Boolean) extends NotebookIOForExecution {

  def toBodyMap: util.HashMap[String, Any] = {
    val map = new util.HashMap[String, Any]()
    map.put("table_name", tableName)
    map.put("schema_name", schemaName)
    map.put("database_name", databaseName)
    map.put("overwrite", overwrite)
    map
  }
}
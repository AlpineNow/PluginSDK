/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import java.util

import com.alpine.plugin.core.annotation.AlpineSdkApi

import scala.collection.JavaConversions

/**
 * :: AlpineSdkApi ::
 */
@AlpineSdkApi
case class TabularSchema(
  definedColumns: Seq[ColumnDef],
  isPartial: Boolean) {

  def getNumDefinedColumns: Int = definedColumns.length
  def getDefinedColumns: Seq[ColumnDef] = definedColumns
  def getDefinedColumnsAsJavaList: util.List[ColumnDef] = JavaConversions.seqAsJavaList(definedColumns)
  def columnNames: Seq[String] = definedColumns.map(_.columnName)
  def columnTypes: Seq[ColumnType.TypeValue] = definedColumns.map(_.columnType)
  def nameDefMap: Map[String, ColumnDef] = Map[String, ColumnDef](definedColumns.map(colDef => colDef.columnName -> colDef): _*)
  // Returns the selected subset of columns
  def select(namesToSelect: Seq[String]): Seq[ColumnDef] = {
    definedColumns.filter(colDef => namesToSelect.contains(colDef.columnName))
  }

}

object TabularSchema {
  def apply(columnDefs: Seq[ColumnDef]): TabularSchema = {
    TabularSchema(columnDefs, isPartial = false)
  }

  @deprecated("Use overloaded method without expectedOutputFormatAttributes.")
  def apply(columnDefs: Seq[ColumnDef],
            expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(columnDefs, isPartial = false)
  }

  def apply(columnDefs: util.List[ColumnDef]): TabularSchema = {
    apply(columnDefs, isPartial = false)
  }

  @deprecated("Use overloaded method without expectedOutputFormatAttributes.")
  def apply(columnDefs: util.List[ColumnDef],
            expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    apply(columnDefs, isPartial = false)
  }

  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial)
  }

  @deprecated("Use overloaded method without expectedOutputFormatAttributes.")
  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial)
  }

  @deprecated("Use overloaded method without expectedOutputFormatAttributes.")
  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: Option[TabularFormatAttributes]): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial)
  }

  @deprecated("Use overloaded method without expectedOutputFormatAttributes.")
  def apply(
    columnDefs: Seq[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(columnDefs, isPartial)
  }

  def empty(isPartial: Boolean): TabularSchema = {
    TabularSchema(Seq(), isPartial)
  }

}

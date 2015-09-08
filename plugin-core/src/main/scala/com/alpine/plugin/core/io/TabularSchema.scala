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
case class TabularSchema private (
  definedColumns: Seq[ColumnDef],
  isPartial: Boolean,
  expectedOutputFormatAttributes: Option[TabularFormatAttributes]) {

  def getNumDefinedColumns: Int = definedColumns.length
  def getDefinedColumns: Seq[ColumnDef] = definedColumns

  /**
   * This may be empty (the user is not required to specify the format).
   * @return TabularFormatAttributes Some(Avro, Parquet, TSV etc) or None.
   */
  def getExpectedOutputFormat: Option[TabularFormatAttributes] = {
    this.expectedOutputFormatAttributes
  }
}

object TabularSchema {
  def apply(columnDefs: Seq[ColumnDef]): TabularSchema = {
    TabularSchema(columnDefs, isPartial = false, None)
  }

  def apply(columnDefs: Seq[ColumnDef],
            expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(columnDefs, isPartial = false, Some(expectedOutputFormatAttributes))
  }

  def apply(columnDefs: util.List[ColumnDef]): TabularSchema = {
    apply(columnDefs, isPartial = false)
  }

  def apply(columnDefs: util.List[ColumnDef],
            expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    apply(columnDefs, isPartial = false, expectedOutputFormatAttributes)
  }

  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial, None)
  }

  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial, Some(expectedOutputFormatAttributes))
  }

  def apply(
    columnDefs: util.List[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: Option[TabularFormatAttributes]): TabularSchema = {
    TabularSchema(JavaConversions.asScalaBuffer(columnDefs), isPartial, expectedOutputFormatAttributes)
  }

  def apply(
    columnDefs: Seq[ColumnDef],
    isPartial: Boolean,
    expectedOutputFormatAttributes: TabularFormatAttributes): TabularSchema = {
    TabularSchema(columnDefs, isPartial, Some(expectedOutputFormatAttributes))
  }
}

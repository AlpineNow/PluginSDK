/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io._

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HdfsDelimitedTabularDataset.
 */
abstract class AbstractHdfsDelimitedTabularDataset(
  val path: String,
  val tabularSchema: TabularSchema,
  val tsvAttributes: TSVAttributes,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef]
) extends HdfsDelimitedTabularDataset {
  override def displayName: String = path
}

case class HdfsDelimitedTabularDatasetDefault(
  override val path: String,
  override val tabularSchema: TabularSchema,
  override val tsvAttributes: TSVAttributes,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends AbstractHdfsDelimitedTabularDataset(path, tabularSchema, tsvAttributes, sourceOperatorInfo, addendum)


object HdfsDelimitedTabularDatasetDefault {
  def apply(path: String,
            tabularSchema: TabularSchema,
            delimiter: Char,
            escapeStr: Char,
            quoteStr: Char,
            containsHeader: Boolean,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef]
             ): HdfsDelimitedTabularDatasetDefault = {
    HdfsDelimitedTabularDatasetDefault(
      path,
      tabularSchema,
      TSVAttributes(delimiter, escapeStr, quoteStr, containsHeader),
      sourceOperatorInfo,
      addendum
    )
  }
}


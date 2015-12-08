/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io._

/**
  * Abstract implementation of [[HdfsDelimitedTabularDataset]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsDelimitedTabularDatasetDefault]].
  * @param path Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param tsvAttributes Attributes describing the particular TSV format.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
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

/**
  * Default implementation of [[HdfsDelimitedTabularDataset]].
  * @param path Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param tsvAttributes Attributes describing the particular TSV format.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
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


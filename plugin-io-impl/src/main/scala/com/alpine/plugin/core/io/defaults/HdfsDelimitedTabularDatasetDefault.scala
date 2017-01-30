/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io._

/**
  * Abstract implementation of [[HdfsDelimitedTabularDataset]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsDelimitedTabularDatasetDefault]].
  *
  * @param path          Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param tsvAttributes Attributes describing the particular CSV format.
  * @param addendum      Map containing additional information.
  */
abstract class AbstractHdfsDelimitedTabularDataset(val path: String,
                                                   val tabularSchema: TabularSchema,
                                                   val tsvAttributes: TSVAttributes,
                                                   val addendum: Map[String, AnyRef]
                                                  ) extends HdfsDelimitedTabularDataset

/**
  * Default implementation of [[HdfsDelimitedTabularDataset]].
  *
  * @param path          Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param tsvAttributes Attributes describing the particular CSV format.
  * @param addendum      Map containing additional information.
  */
case class HdfsDelimitedTabularDatasetDefault(override val path: String,
                                              override val tabularSchema: TabularSchema,
                                              override val tsvAttributes: TSVAttributes,
                                              override val addendum: Map[String, AnyRef]
                                             ) extends AbstractHdfsDelimitedTabularDataset(path, tabularSchema, tsvAttributes, addendum) {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def this(path: String,
           tabularSchema: TabularSchema,
           tsvAttributes: TSVAttributes,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()
          ) = {
    this(path, tabularSchema, tsvAttributes, addendum)
  }
}


object HdfsDelimitedTabularDatasetDefault {

  @deprecated("Use constructor without sourceOperatorInfo and with tsvAttributes.")
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
      addendum
    )
  }

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(path: String,
            tabularSchema: TabularSchema,
            tsvAttributes: TSVAttributes,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef] = Map[String, AnyRef]()
           ): HdfsDelimitedTabularDatasetDefault = {
    HdfsDelimitedTabularDatasetDefault(path, tabularSchema, tsvAttributes, addendum)
  }

  def apply(path: String,
            tabularSchema: TabularSchema,
            tsvAttributes: TSVAttributes
           ): HdfsDelimitedTabularDatasetDefault = {
    HdfsDelimitedTabularDatasetDefault(path, tabularSchema, tsvAttributes, Map[String, AnyRef]())
  }

}


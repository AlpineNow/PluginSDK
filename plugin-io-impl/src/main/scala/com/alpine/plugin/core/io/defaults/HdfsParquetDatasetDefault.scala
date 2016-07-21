/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsParquetDataset, OperatorInfo, TabularSchema}

/**
  * Abstract implementation of [[HdfsParquetDataset]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsParquetDatasetDefault]].
  *
  * @param path          Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum      Map containing additional information.
  */
abstract class AbstractHdfsParquetDataset(val path: String,
                                          val tabularSchema: TabularSchema,
                                          val addendum: Map[String, AnyRef]
                                         )
  extends HdfsParquetDataset

/**
  * Default implementation of [[HdfsParquetDataset]].
  *
  * @param path          Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum      Map containing additional information.
  */
case class HdfsParquetDatasetDefault(override val path: String,
                                     override val tabularSchema: TabularSchema,
                                     override val addendum: Map[String, AnyRef])
  extends AbstractHdfsParquetDataset(path, tabularSchema, addendum) {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def this(path: String,
           tabularSchema: TabularSchema,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef]) = {
    this(path, tabularSchema, addendum)
  }
}

object HdfsParquetDatasetDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(path: String,
            tabularSchema: TabularSchema,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsParquetDatasetDefault = {
    HdfsParquetDatasetDefault(path, tabularSchema, addendum)
  }

  def apply(path: String,
            tabularSchema: TabularSchema): HdfsParquetDatasetDefault = {
    HdfsParquetDatasetDefault(path, tabularSchema, Map[String, AnyRef]())
  }

}

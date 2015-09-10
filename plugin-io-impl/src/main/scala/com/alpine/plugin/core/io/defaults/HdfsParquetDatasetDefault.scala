/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsParquetDataset, OperatorInfo, TabularSchema}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HdfsParquetDataset.
 */
abstract class AbstractHdfsParquetDataset(val path: String,
                                          val tabularSchema: TabularSchema,
                                          val sourceOperatorInfo: Option[OperatorInfo],
                                          val addendum: Map[String, AnyRef]
                                           )
  extends HdfsParquetDataset {
  override def displayName: String = path
}

case class HdfsParquetDatasetDefault(override val path: String,
                                     override val tabularSchema: TabularSchema,
                                     override val sourceOperatorInfo: Option[OperatorInfo],
                                     override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractHdfsParquetDataset(path, tabularSchema, sourceOperatorInfo, addendum)

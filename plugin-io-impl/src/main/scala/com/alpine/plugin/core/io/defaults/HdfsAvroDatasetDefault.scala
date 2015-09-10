/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsAvroDataset, OperatorInfo, TabularSchema}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HdfsAvroDataset.
 */
abstract class AbstractHdfsAvroDataset(
  val path: String,
  val tabularSchema: TabularSchema,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef]
) extends HdfsAvroDataset {
    override def displayName: String = path
}

case class HdfsAvroDatasetDefault(
  override val path: String,
  override val tabularSchema: TabularSchema,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends AbstractHdfsAvroDataset(path, tabularSchema, sourceOperatorInfo, addendum)
/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsAvroDataset, OperatorInfo, TabularSchema}

/**
  * Abstract implementation of [[HdfsAvroDataset]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsAvroDatasetDefault]].
  * @param path Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractHdfsAvroDataset(
  val path: String,
  val tabularSchema: TabularSchema,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef]
) extends HdfsAvroDataset {
    override def displayName: String = path
}

/**
  * Default implementation of [[HdfsAvroDataset]].
  * @param path Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class HdfsAvroDatasetDefault(
  override val path: String,
  override val tabularSchema: TabularSchema,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends AbstractHdfsAvroDataset(path, tabularSchema, sourceOperatorInfo, addendum)
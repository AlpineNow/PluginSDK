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
  * @param addendum Map containing additional information.
  */
abstract class AbstractHdfsAvroDataset(
  val path: String,
  val tabularSchema: TabularSchema,
  val addendum: Map[String, AnyRef]
) extends HdfsAvroDataset

/**
  * Default implementation of [[HdfsAvroDataset]].
  * @param path Path of the file in HDFS.
  * @param tabularSchema Description of the column structure of the file.
  * @param addendum Map containing additional information.
  */
case class HdfsAvroDatasetDefault(
  override val path: String,
  override val tabularSchema: TabularSchema,
  override val addendum: Map[String, AnyRef]
) extends AbstractHdfsAvroDataset(path, tabularSchema, addendum) {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def this(path: String,
           tabularSchema: TabularSchema,
           sourceOperatorInfo: Option[OperatorInfo],
           addendum: Map[String, AnyRef] = Map[String, AnyRef]()) = {
    this(path, tabularSchema, addendum)
  }
}

object HdfsAvroDatasetDefault {

  @deprecated("Use constructor without sourceOperatorInfo.")
  def apply(path: String,
            tabularSchema: TabularSchema,
            sourceOperatorInfo: Option[OperatorInfo],
            addendum: Map[String, AnyRef] = Map[String, AnyRef]()): HdfsAvroDatasetDefault = {
    HdfsAvroDatasetDefault(path, tabularSchema, addendum)
  }

  def apply(path: String,
            tabularSchema: TabularSchema): HdfsAvroDatasetDefault = {
    HdfsAvroDatasetDefault(path, tabularSchema, Map[String, AnyRef]())
  }

}
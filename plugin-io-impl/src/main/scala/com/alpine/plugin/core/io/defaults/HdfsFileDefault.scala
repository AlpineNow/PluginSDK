/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsFile, OperatorInfo}

/**
  * Abstract implementation of [[HdfsFile]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsFileDefault]].
  * @param path Path of the file in HDFS.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
abstract class AbstractHdfsFile(val path: String,
                                val sourceOperatorInfo: Option[OperatorInfo],
                                val addendum: Map[String, AnyRef])
  extends HdfsFile {
  override def displayName: String = path
}

/**
  * Default implementation of [[HdfsFile]].
  * @param path Path of the file in HDFS.
  * @param sourceOperatorInfo Information about the operator that generated this object as output.
  * @param addendum Map containing additional information.
  */
case class HdfsFileDefault(override val path: String,
                           override val sourceOperatorInfo: Option[OperatorInfo],
                           override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractHdfsFile(path, sourceOperatorInfo, addendum)

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsFile, OperatorInfo}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HdfsFileDefault.
 */
abstract class AbstractHdfsFile(val path: String,
                                val sourceOperatorInfo: Option[OperatorInfo],
                                val addendum: Map[String, AnyRef])
  extends HdfsFile {
  override def displayName: String = path
}

case class HdfsFileDefault(override val path: String,
                           override val sourceOperatorInfo: Option[OperatorInfo],
                           override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractHdfsFile(path, sourceOperatorInfo, addendum)

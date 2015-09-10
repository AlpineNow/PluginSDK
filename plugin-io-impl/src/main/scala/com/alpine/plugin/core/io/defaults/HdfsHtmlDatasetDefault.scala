/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.{HdfsHtmlDataset, OperatorInfo}

/**
 * Default implementation.
 * Developers wanting to change behaviour can extend HdfsHtmlDataset.
 */
abstract class AbstractHdfsHtmlDataset(val path: String,
                                       val sourceOperatorInfo: Option[OperatorInfo],
                                       val addendum: Map[String, AnyRef])
  extends HdfsHtmlDataset {
  override def displayName: String = path
}

case class HdfsHtmlDatasetDefault(override val path: String,
                                  override val sourceOperatorInfo: Option[OperatorInfo],
                                  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]())
  extends AbstractHdfsHtmlDataset(path, sourceOperatorInfo, addendum)

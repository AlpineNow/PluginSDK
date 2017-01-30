/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.io.defaults

import com.alpine.plugin.core.io.HdfsRawTextDataset

/**
  * Abstract implementation of [[HdfsRawTextDataset]].
  * Can be extended by developers who want custom behaviour not provided by [[HdfsRawTextDatasetDefault]].
  *
  * @param path     Path of the file in HDFS.
  * @param addendum Map containing additional information.
  */
abstract class AbstractHdfsRawTextDataset(val path: String,
                                          val addendum: Map[String, AnyRef]
                                         )
  extends HdfsRawTextDataset

/**
  * Default implementation of [[HdfsRawTextDataset]].
  *
  * @param path     Path of the file in HDFS.
  * @param addendum Map containing additional information.
  */
case class HdfsRawTextDatasetDefault(override val path: String,
                                     override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
                                    )
  extends AbstractHdfsRawTextDataset(path, addendum)
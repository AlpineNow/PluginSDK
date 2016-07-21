/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * A tabular dataset trait for Hadoop-based datasets. This is not an IOBase interface by itself.
 * Developers should not directly extend/implement this trait.
 * Developers who want to implement their own TabularDataset formats that
 * reside in Hdfs should extend HdfsTabularDataset.
 */
@AlpineSdkApi
trait TabularDataset extends IOBase {
  def tabularSchema: TabularSchema
}

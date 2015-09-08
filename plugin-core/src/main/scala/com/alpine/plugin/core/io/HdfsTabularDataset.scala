/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * Tabular dataset refers to table format datasets, such as CSV/TSV/
 * Parquet/Avro, etc. that reside within Hdfs.
 */
@AlpineSdkApi
trait HdfsTabularDataset extends HdfsFile with TabularDataset

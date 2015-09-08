/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * An unformatted text file stored on HDFS.
 * Use this for accessing raw text files.
 */
@AlpineSdkApi
trait HdfsRawTextDataset extends HdfsFile
/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */
package com.alpine.plugin.core.serialization.json

import java.lang.reflect.Type

import com.alpine.plugin.core.io._

object CoreJsonUtil {

  def typesNeedingInterfaceAdapters: Seq[Type] = Seq[Type](
    classOf[DBTable],
    classOf[HdfsDelimitedTabularDataset],
    classOf[HdfsFile],
    classOf[HdfsHtmlDataset],
    classOf[HdfsRawTextDataset],
    classOf[HdfsTabularDataset],
    classOf[HdfsParquetDataset],
    classOf[HdfsAvroDataset],
    classOf[TabularDataset],
    classOf[HiveTable],
    classOf[IONone],
    classOf[IOList[_]],
    classOf[IOString],
    classOf[IOBase],
    classOf[Tuple],
    classOf[Tuple2[_, _]],
    classOf[Tuple3[_, _, _]],
    classOf[Tuple4[_, _, _, _]]
  )

}

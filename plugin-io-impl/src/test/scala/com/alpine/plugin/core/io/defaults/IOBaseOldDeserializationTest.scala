/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.io.defaults

import java.lang.reflect.Type

import com.alpine.plugin.core.io.{ColumnDef, ColumnType, HdfsDelimitedTabularDataset, HdfsFile, IOBase, IOList, OperatorInfo, TSVAttributes, TabularSchema, Tuple2, Tuple3, Tuple4}
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

/**
  * Deserialization test for IOBase objects predating the removal of the sourceOperatorInfo and displayName fields.
  * (pre 1.7-SDK).
  */
class IOBaseOldDeserializationTest extends AbstractIOBaseDeserialization {

  val schema: TabularSchema = TabularSchema(Seq(ColumnDef("name1", ColumnType.Long)))
  val opInfo = OperatorInfo("uuid", "name")

  def makeDBTable = DBTableDefault("schemaName", "tableName", schema, isView = false, "x", "y", None, Map[String, AnyRef]())

  test("Should serialize DBTableDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.DBTableDefault","data":{"schemaName":"schemaName","tableName":"tableName","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"isView":false,"dbName":"x","dbURL":"y","sourceOperatorInfo":{"type":"None"},"addendum":{}}}"""
    testSerialization(makeDBTable, Some(previousJson))
  }

  val avroDataset = HdfsAvroDatasetDefault("path", schema, Some(opInfo))
  test("Should serialize HdfsAvroDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsAvroDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(avroDataset, Some(previousJson))
  }


  test("Should serialize dictionary correctly") {
    val addendum = Map(
      // TODO: This fails. Bug.
      // "localTable" -> localTable,
      "3" -> "Hi"
    )
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.DBTableDefault","data":{"schemaName":"schemaName","tableName":"tableName","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"isView":false,"dbName":"x","dbURL":"y","sourceOperatorInfo":{"type":"None"},"addendum":{"3":"Hi"}}}"""
    val dBTable = DBTableDefault("schemaName", "tableName", schema, isView = false, "x", "y", None, addendum)
    testSerialization(dBTable, Some(previousJson))
  }

  val tsvAttributes = TSVAttributes(',', '\\', '`', containsHeader = false, null)
  val hdfsDataset = HdfsDelimitedTabularDatasetDefault("path", schema, tsvAttributes, Some(opInfo))
  test("Should serialize HdfsDelimitedTabularDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(hdfsDataset, Some(previousJson))
  }

  val hdfsFile: HdfsFile = HdfsFileDefault("path")
  test("Should serialize HdfsFileDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsFileDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(hdfsFile, Some(previousJson))
  }

  test("Should serialize IONone as an interface correctly") {
    val ioNone = IONoneDefault()
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IONoneDefault","data":{"sourceOperatorInfo":{"type":"None"},"addendum":{}}}"""
    testSerialization(ioNone, Some(previousJson))
  }

  test("Should serialize HdfsHtmlDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsHtmlDatasetDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HdfsHtmlDatasetDefault("path"), Some(previousJson))
  }

  test("Should serialize HdfsParquetDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsParquetDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HdfsParquetDatasetDefault("path", schema, Some(opInfo)), Some(previousJson))
  }

  test("Should serialize HdfsRawTextDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsRawTextDatasetDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HdfsRawTextDatasetDefault("path"), Some(previousJson))
  }

  test("Should serialize HiveTableDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HiveTableDefault","data":{"tableName":"tableName","dbName":{"type":"Some","data":"dbName"},"tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HiveTableDefault("tableName", Some("dbName"), schema), Some(previousJson))
  }

  test("Should serialize IOListDefault correctly") {
    val iOListDefault = IOListDefault[HdfsDelimitedTabularDataset](
      Seq(
        HdfsDelimitedTabularDatasetDefault("path2", schema, tsvAttributes, Some(opInfo)),
        hdfsDataset
      ),
      Seq() // Old IOList did not have sources.
    )
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[IOList[_]], Array[Type](classOf[HdfsDelimitedTabularDataset]), null)
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IOListDefault","data":{"displayName":"path","elements":[{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path2","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}],"sourceOperatorInfo":{"type":"None"},"sources":[],"addendum":{}}} """
    testSerialization(iOListDefault, Some(previousJson), pType)
  }

  test("Should serialize Tuple2Default correctly") {
    val tuple = new Tuple2Default("displayName", hdfsFile, hdfsDataset, Some(opInfo))
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[Tuple2[_, _]], Array[Type](classOf[IOBase], classOf[IOBase]), null)
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.Tuple2Default","data":{"displayName":"displayName","_1":{"type":"com.alpine.plugin.core.io.defaults.HdfsFileDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"_2":{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}} """
    testSerialization(tuple, Some(previousJson), pType)
  }

  test("Should serialize Tuple3Default correctly") {
    val tuple = new Tuple3Default("displayName", hdfsFile, hdfsDataset, makeDBTable, Some(opInfo))
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[Tuple3[_, _, _]], Array[Type](classOf[IOBase], classOf[IOBase], classOf[IOBase]), null)
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.Tuple3Default","data":{"displayName":"displayName","_1":{"type":"com.alpine.plugin.core.io.defaults.HdfsFileDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"_2":{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"_3":{"type":"com.alpine.plugin.core.io.defaults.DBTableDefault","data":{"schemaName":"schemaName","tableName":"tableName","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"isView":false,"dbName":"x","dbURL":"y","sourceOperatorInfo":{"type":"None"},"addendum":{}}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(tuple, Some(previousJson), pType)
  }

  test("Should serialize Tuple4Default correctly") {
    val tuple = new Tuple4Default("displayName", hdfsFile, hdfsDataset, makeDBTable, new IOStringDefault("Raspberries", Some(opInfo)), Some(opInfo))
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[Tuple4[_, _, _, _]], Array[Type](classOf[IOBase], classOf[IOBase], classOf[IOBase], classOf[IOBase]), null)
    val previousJson =
      """{"type":"com.alpine.plugin.core.io.defaults.Tuple4Default","data":{"displayName":"displayName",
        |"_1":{"type":"com.alpine.plugin.core.io.defaults.HdfsFileDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},
        |"_2":{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},
        |"_3":{"type":"com.alpine.plugin.core.io.defaults.DBTableDefault","data":{"schemaName":"schemaName","tableName":"tableName","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"isView":false,"dbName":"x","dbURL":"y","sourceOperatorInfo":{"type":"None"},"addendum":{}}},
        |"_4":{"type":"com.alpine.plugin.core.io.defaults.IOStringDefault","data":{"value":"Raspberries","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}
        |,"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}""".stripMargin
    testSerialization(tuple, Some(previousJson), pType)
  }

  test("Should serialize IOString correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IOStringDefault","data":{"value":"Raspberries","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(new IOStringDefault("Raspberries", Some(opInfo)), Some(previousJson))
  }

  test("Should serialize IONone correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IONoneDefault","data":{"sourceOperatorInfo":{"type":"None"},"addendum":{}}}"""
    testSerialization(IONoneDefault(), Some(previousJson))
  }

}

package com.alpine.plugin.core.io.defaults
import java.lang.reflect.Type

import com.alpine.common.serialization.json.JsonUtil
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.serialization.json.CoreJsonUtil
import org.scalatest.FunSuite
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

/**
 * TODO: Refactor serialization code so this can be in plugin-io-impl.
 */
class IOBaseDefaultSerializationTest extends FunSuite {

  val gson = JsonUtil.gsonBuilderWithInterfaceAdapters(CoreJsonUtil.typesNeedingInterfaceAdapters).create

  def testSerialization[T <: IOBase](ioBase: T, previousJson: Option[String] = None, t: Type = classOf[IOBase]): T = {
    val json = gson.toJson(ioBase, t)
    val newObj: IOBase = gson.fromJson(json, t)

    assert(ioBase === newObj)
    assert(ioBase.addendum === newObj.addendum)
    previousJson match {
      case Some(s) =>
        val oldObjDeserialized: IOBase = gson.fromJson(previousJson.get, t)
        assert(ioBase === oldObjDeserialized)
        assert(ioBase.addendum === oldObjDeserialized.addendum)
      case None =>
        println("You should include the following json as the previousJson argument to test stability across releases.")
        println(json)
    }
    newObj.asInstanceOf[T]
  }

  val schema: TabularSchema = TabularSchema(Seq(ColumnDef("name1", ColumnType.Long)))
  val opInfo = new OperatorInfo("uuid", "name")

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

  val hdfsFile: HdfsFile = HdfsFileDefault("path", Some(opInfo))
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
    testSerialization(HdfsHtmlDatasetDefault("path", Some(opInfo)), Some(previousJson))
  }

  test("Should serialize HdfsParquetDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsParquetDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HdfsParquetDatasetDefault("path", schema, Some(opInfo)), Some(previousJson))
  }

  test("Should serialize HdfsRawTextDatasetDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HdfsRawTextDatasetDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HdfsRawTextDatasetDefault("path", Some(opInfo)), Some(previousJson))
  }

  test("Should serialize HiveTableDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.HiveTableDefault","data":{"tableName":"tableName","dbName":{"type":"Some","data":"dbName"},"tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(HiveTableDefault("tableName", Some("dbName"), schema, Some(opInfo)), Some(previousJson))
  }

  val rows = Seq(Row(Seq("a", "b")), Row(Seq("c", "d")))
  val localTable: LocalTableDefault = new LocalTableDefault("tableName", rows, Some(opInfo))

  test("Should serialize LocalTableDefault correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.LocalTableDefault","data":{"tableName":"tableName","rows":[{"values":["a","b"]},{"values":["c","d"]}],"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    val newTable = testSerialization(localTable, Some(previousJson))
    // Call this to check the rows fields is instantiated properly on the AbstractLocalTable level.
    newTable.getNumRows
  }


  test("Should serialize IOListDefault correctly") {
    val iOListDefault = new IOListDefault[HdfsDelimitedTabularDataset]("path",
      Seq(
        HdfsDelimitedTabularDatasetDefault("path2", schema, tsvAttributes, Some(opInfo)),
        hdfsDataset
      )
    )
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[IOList[_]], Array[Type](classOf[HdfsDelimitedTabularDataset]), null)
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IOListDefault","data":{"displayName":"path","elements":[{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path2","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}],"sourceOperatorInfo":{"type":"None"},"addendum":{}}} """
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
    val tuple = new Tuple4Default("displayName", hdfsFile, hdfsDataset, makeDBTable, localTable, Some(opInfo))
    val pType: ParameterizedTypeImpl = ParameterizedTypeImpl.make(classOf[Tuple4[_, _, _, _]], Array[Type](classOf[IOBase], classOf[IOBase], classOf[IOBase], classOf[IOBase]), null)
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.Tuple4Default","data":{"displayName":"displayName","_1":{"type":"com.alpine.plugin.core.io.defaults.HdfsFileDefault","data":{"path":"path","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"_2":{"type":"com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault","data":{"path":"path","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"tsvAttributes":{"delimiter":",","escapeStr":"\\","quoteStr":"`","containsHeader":false},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"_3":{"type":"com.alpine.plugin.core.io.defaults.DBTableDefault","data":{"schemaName":"schemaName","tableName":"tableName","tabularSchema":{"definedColumns":[{"columnName":"name1","columnType":"Long"}],"isPartial":false,"expectedOutputFormatAttributes":{"type":"None"}},"isView":false,"dbName":"x","dbURL":"y","sourceOperatorInfo":{"type":"None"},"addendum":{}}},"_4":{"type":"com.alpine.plugin.core.io.defaults.LocalTableDefault","data":{"tableName":"tableName","rows":[{"values":["a","b"]},{"values":["c","d"]}],"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}},"sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(tuple, Some(previousJson), pType)
  }

  test("Should serialize IOString correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IOStringDefault","data":{"value":"Raspberries","sourceOperatorInfo":{"type":"Some","data":{"uuid":"uuid","name":"name"}},"addendum":{}}}"""
    testSerialization(IOStringDefault("Raspberries", Some(opInfo)), Some(previousJson))
  }

  test("Should serialize IONone correctly") {
    val previousJson = """{"type":"com.alpine.plugin.core.io.defaults.IONoneDefault","data":{"sourceOperatorInfo":{"type":"None"},"addendum":{}}}"""
    testSerialization(IONoneDefault(), Some(previousJson))
  }

}


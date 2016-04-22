package com.alpine.plugin.test.sparktests

import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.io.{TSVAttributes, ColumnDef, ColumnType, TabularSchema}
import com.alpine.plugin.core.spark.utils.{SparkSqlDateTimeUtils, SparkRuntimeUtils}
import com.alpine.plugin.test.utils.TestSparkContexts
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

import scala.collection.mutable


class DateUtilsTest  extends FunSuite {

  import TestSparkContexts._

  val sparkUtils = new SparkRuntimeUtils(sc)
  val path = "plugin-test/src/test/resources/WeirdDates.csv"

  val justDateType = ColumnType.DateTime("dd/MM/yyyy")
  val justTime = ColumnType.DateTime("HH:mm")
  val standardDateType = ColumnType.DateTime(SparkSqlDateTimeUtils.SPARK_SQL_DATE_FORMAT)

  val dateSchema = TabularSchema(Seq(
    ColumnDef("JustDate", justDateType),
    ColumnDef("JustTime", justTime),
    ColumnDef("RowValue", ColumnType.String),
    ColumnDef("StandardDateType", standardDateType)))

  val dateFormatMap = mutable.Map(
    "JustDate" -> justDateType.format.get,
    "JustTime" -> justTime.format.get,
    "StandardDateType" -> standardDateType.format.get)

  val sqlSchema = StructType(Array(
    StructField("JustDate", StringType),
    StructField("JustTime", StringType),
    StructField("RowValue", StringType),
    StructField("StandardDateType", StringType)))

  val rows = Seq("12/07/1991,6:30,1,1991-12-07",
    "07/07/1991,5:25,2,1991-07-07").map(r => sql.Row.fromSeq(r.split(",")))

  test("DateCorrect Method "){
    val df = sqlContext.createDataFrame(sc.parallelize(rows), sqlSchema)

    val asDates = sparkUtils.mapDFtoUnixDateTime(df, dateFormatMap)

    val result = asDates.collect()
    assert(result.forall(!_.anyNull))
  }

  test("Read in Date Dataframe"){
    val df = sparkUtils.getDataFrame(HdfsDelimitedTabularDatasetDefault(path, dateSchema, TSVAttributes.defaultCSV, None))
    val result = df.collect()
    assert(!result.exists(_.anyNull))
  }

  test("Print pretty dates "){
    val df = sparkUtils.getDataFrame(HdfsDelimitedTabularDatasetDefault(path, dateSchema, TSVAttributes.defaultCSV, None))
    sparkUtils.deleteFilePathIfExists("target/test-results/sparkUtilsDateTest")
    val result = sparkUtils.saveAsCSV("target/test-results/sparkUtilsDateTest",
      df ,TSVAttributes.defaultCSV ,None,  Map[String, AnyRef]())
  }

  test("Round trip date conversion"){
    val df = sqlContext.createDataFrame(sc.parallelize(rows), sqlSchema)
    val asDates = sparkUtils.mapDFtoUnixDateTime(df, dateFormatMap)

    val map = sparkUtils.getDateMap(dateSchema)
    val correctedDF = sparkUtils.mapDFtoCustomDateTimeFormat(asDates, map)
    val result = correctedDF.collect()
    assert(result.head(0).toString.equals("12/07/1991"))
    assert(result.forall(!_.anyNull))
  }
}


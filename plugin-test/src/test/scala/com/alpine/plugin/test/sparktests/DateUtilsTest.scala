package com.alpine.plugin.test.sparktests

import com.alpine.plugin.core.io.defaults.HdfsDelimitedTabularDatasetDefault
import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TSVAttributes, TabularSchema}
import com.alpine.plugin.core.spark.utils.{SparkSchemaUtils, SparkRuntimeUtils, SparkSqlDateTimeUtils}
import com.alpine.plugin.test.utils.TestSparkContexts
import org.apache.spark._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class DateUtilsTest extends FunSuite {

  import TestSparkContexts._

  val sparkUtils = new SparkRuntimeUtils(sc)

  val path = "plugin-test/src/test/resources/WeirdDates.csv"

  val justDateType = ColumnType.DateTime("dd/MM/yyyy")
  val justTime = ColumnType.DateTime("HH:mm")
  val standardDateType = ColumnType.DateTime(ColumnType.SPARK_SQL_DATE_FORMAT)

  val dateSchema = TabularSchema(Seq(
    ColumnDef("JustDate", justDateType),
    ColumnDef("JustTime", justTime),
    ColumnDef("RowValue", ColumnType.String),
    ColumnDef("StandardDateType", standardDateType)))

  val dateFormatMap = dateSchema.definedColumns
    .filter(_.columnType.format.isDefined)
    .map(column => (column.columnName, column.columnType.format.get)).toMap

  val sqlSchema = StructType(Array(
    StructField("JustDate", StringType),
    StructField("JustTime", StringType),
    StructField("RowValue", StringType),
    StructField("StandardDateType", StringType)))

  val rows = Seq("12/07/1991,6:30,1,1991-12-07",
    "07/07/1991,5:25,2,1991-07-07").map(r => sql.Row.fromSeq(r.split(",")))

  test("DateCorrect Method ") {
    val df = sqlContext.createDataFrame(sc.parallelize(rows), sqlSchema)

    val asDates = sparkUtils.mapDFtoUnixDateTime(df, dateFormatMap)

    val result = asDates.collect()
    assert(result.forall(!_.anyNull))
  }

  test("Read in Date Dataframe") {
    val df = sparkUtils.getDataFrame(HdfsDelimitedTabularDatasetDefault(path, dateSchema, TSVAttributes.defaultCSV, None))
    val result = df.collect()
    assert(!result.exists(_.anyNull))
  }

  test("Print pretty dates ") {
    val df = sparkUtils.getDataFrame(HdfsDelimitedTabularDatasetDefault(path, dateSchema, TSVAttributes.defaultCSV, None))
    sparkUtils.deleteFilePathIfExists("target/test-results/sparkUtilsDateTest")
    val result = sparkUtils.saveAsCSV("target/test-results/sparkUtilsDateTest",
      df, TSVAttributes.defaultCSV, None, Map[String, AnyRef]())
  }

  test("Round trip date conversion") {
    val df = sqlContext.createDataFrame(sc.parallelize(rows), sqlSchema)
    val asDates = sparkUtils.mapDFtoUnixDateTime(df, dateFormatMap)
    val map = sparkUtils.getDateMap(dateSchema)
    val correctedDF = sparkUtils.mapDFtoCustomDateTimeFormat(asDates, map)
    val result = correctedDF.collect()
    assert(result.head(0).toString.equals("12/07/1991"))
    assert(result.forall(!_.anyNull))
  }

  test("Convert Spark Schema to Alpine Schema") {
    val schemaUtils = SparkSchemaUtils
    val myNewSparkSchema = StructType(
      Array(
        StructField("Grade", StringType),
        SparkSqlDateTimeUtils.addDateFormatInfo(StructField("EuropeanDate", DateType), "dd/MM/yyyy"),
        StructField("DefaultDateType", DateType),
        SparkSqlDateTimeUtils.addDateFormatInfo(
          StructField("TimeOnly", TimestampType), "HH:mm"),
        StructField("DefaultTimeStamp", TimestampType)
      )
    )

    val expectedAlpineTypes = Seq(
      ColumnType.String,
      ColumnType.DateTime("dd/MM/yyyy"),
      ColumnType.DateTime(ColumnType.SPARK_SQL_DATE_FORMAT),
      ColumnType.DateTime("HH:mm"),
      ColumnType.DateTime(ColumnType.SPARK_SQL_TIME_STAMP_FORMAT)
    )

    val alpineSchema = schemaUtils.convertSparkSQLSchemaToTabularSchema(myNewSparkSchema)


    assert(alpineSchema.getDefinedColumns.map(_.columnType) === expectedAlpineTypes)

    //round trip to Spark Schema
    // when we convert the alpine schema we always return time stampe types
    val timeStampOnlySchema = StructType(
      Array(
        StructField("Grade", StringType),
        SparkSqlDateTimeUtils.addDateFormatInfo(StructField("EuropeanDate", TimestampType, nullable = true), "dd/MM/yyyy"),
        SparkSqlDateTimeUtils.addDateFormatInfo(
          StructField("DefaultDateType", TimestampType), ColumnType.SPARK_SQL_DATE_FORMAT),
        SparkSqlDateTimeUtils.addDateFormatInfo(
          StructField("TimeOnly", TimestampType), "HH:mm"),
        SparkSqlDateTimeUtils.addDateFormatInfo(
          StructField("DefaultTimeStamp", TimestampType), ColumnType.SPARK_SQL_TIME_STAMP_FORMAT)
      )
    )
    val sparkSchemaFromAlpineSchema = schemaUtils.convertTabularSchemaToSparkSQLSchema(alpineSchema)
    sparkSchemaFromAlpineSchema.fields.zip(timeStampOnlySchema.fields).foreach {
      case (expectedDef, actualDef) =>
        assert(expectedDef.equals(actualDef), expectedDef.toString + " != " + actualDef.toString)
    }
  }

  test("Convert Alpine Schema to Spark Schema") {

    val alpineSchema = TabularSchema(Seq(
      ColumnDef("Grade", ColumnType.String),
      ColumnDef("EuropeanDateType", ColumnType.DateTime("dd/MM/yyy")),
      ColumnDef("DefaultDateType", ColumnType.DateTime(ColumnType.SPARK_SQL_DATE_FORMAT)),
      ColumnDef("TimeOnly", ColumnType.DateTime("HH:mm")),
      ColumnDef("DefaultTimeStamp", ColumnType.DateTime(ColumnType.SPARK_SQL_TIME_STAMP_FORMAT))
    ))

    val sparkSchema = sparkUtils.convertTabularSchemaToSparkSQLSchema(alpineSchema)

    //this one should round trip exactly correct
    val addMedataData = SparkSqlDateTimeUtils.addDateFormatInfo(StructField("DateCol", TimestampType), "dd/MM/yyyy")
    val getMedataData = SparkSqlDateTimeUtils.getDatFormatInfo(addMedataData)
    val roundTripAlpineSchema = sparkUtils.convertSparkSQLSchemaToTabularSchema(sparkSchema)
    assert(SparkSqlDateTimeUtils.getDatFormatInfo(sparkSchema("EuropeanDateType")).get == "dd/MM/yyy")
    alpineSchema.definedColumns.zip(roundTripAlpineSchema.getDefinedColumns).foreach {
      case (expectedDef, actualDef) =>
        assert(expectedDef.equals(actualDef), expectedDef.toString + " != " + actualDef.toString)
    }
  }
}


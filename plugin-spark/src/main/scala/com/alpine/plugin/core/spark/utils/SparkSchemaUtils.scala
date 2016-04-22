package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.io.{ColumnDef, ColumnType, TabularSchema}
import org.apache.spark.sql.types._
import scala.collection.mutable

/**
  * Created by rachelwarren on 4/4/16.
  */
trait SparkSchemaUtils {


  // ======================================================================
  // Schema conversion util functions.
  // ======================================================================

  def convertColumnTypeToSparkSQLDataType(columnType: ColumnType.TypeValue): (DataType, Option[String]) = {
    columnType match {
      case ColumnType.Int => (IntegerType, None)
      case ColumnType.Long => (LongType, None)
      case ColumnType.Float => (FloatType, None)
      case ColumnType.Double => (DoubleType, None)
      case ColumnType.String => (StringType, None)
      case ColumnType.DateTime =>
        columnType.format match {
          case Some(format: String) => (StringType, columnType.format)
          case None => (StringType, None) //should look up what the alpine default is
        }
      case _ => (StringType, columnType.format)
      // Rather than throwing an exception, treat it as a string type.
      // throw new UnsupportedOperationException(columnType.toString + " is not supported.")
    }
  }

  /**
    * Converts from a Spark SQL data type to an Alpine-specific column type.
    */
  def convertSparkSQLDataTypeToColumnType(dataType: DataType): ColumnType.TypeValue = {
    dataType match {
      case IntegerType => ColumnType.Int
      case LongType => ColumnType.Long
      case FloatType => ColumnType.Float
      case DoubleType => ColumnType.Double
      case StringType => ColumnType.String
      case DateType => ColumnType.DateTime
      case TimestampType => ColumnType.DateTime
      case _ =>
        throw new UnsupportedOperationException(
          "Spark SQL data type " + dataType.toString + " is not supported."
        )
    }
  }


  /**
    * Converts from a Spark SQL struct field  to an Alpine-specific column def.
    */
  def convertSparkSQLDataTypeToColumnType(structField: StructField): ColumnDef = {
    val name = structField.name
    val dataType = structField.dataType
    dataType match {
      case IntegerType => ColumnDef(name, ColumnType.Int)
      case LongType => ColumnDef(name, ColumnType.Long)
      case FloatType => ColumnDef(name, ColumnType.Float)
      case DoubleType => ColumnDef(name, ColumnType.Double)
      case StringType => ColumnDef(name, ColumnType.String)
      case DateType =>
        ColumnDef(name, ColumnType.DateTime(
          SparkSqlDateTimeUtils.getDatFormatInfo(structField)
            .getOrElse(SparkSqlDateTimeUtils.SPARK_SQL_DATE_FORMAT)))
      case TimestampType =>
        ColumnDef(name, ColumnType.DateTime(
          SparkSqlDateTimeUtils.getDatFormatInfo(structField)
            .getOrElse(SparkSqlDateTimeUtils.SPARK_SQL_TIME_STAMP_FORMAT)))
      case _ =>
        throw new UnsupportedOperationException(
          "Spark SQL data type " + dataType.toString + " is not supported."
        )
    }
  }

  /**
    * Convert the Alpine 'TabularSchema' with column names and types to the equivalent Spark SQL
    * data frame header.
    * @param tabularSchema An Alpine 'TabularSchemaOutline' object with fixed column definitions
    *                      containing a name and Alpine specific type.
    * @return
    */
  @deprecated("Use convertTabularSchemaToSparkSQLSchemaDateSupport")
  def convertTabularSchemaToSparkSQLSchema(tabularSchema: TabularSchema): StructType = {
    StructType(
      tabularSchema.getDefinedColumns.map {
        columnDef => {

          val (newType, _) = convertColumnTypeToSparkSQLDataType(columnDef.columnType)
          StructField(
            name = columnDef.columnName,
            dataType = newType,
            nullable = true
          )
        }
      }
    )
  }

  /**
    * Convert the Alpine 'TabularSchema' with column names and types to the equivalent Spark SQL
    * data frame header.
    * @param tabularSchema An Alpine 'TabularSchemaOutline' object with fixed column definitions
    *                      containing a name and Alpine specific type.
    * @return
    */
  def convertTabularSchemaToSparkSQLSchemaDateSupport(tabularSchema: TabularSchema): (StructType, mutable.Map[String, String]) = {
    val dateFormats = mutable.Map[String, String]()
    val schema = StructType(
      tabularSchema.getDefinedColumns.map {
        columnDef => {

          val (newType, formatString) = convertColumnTypeToSparkSQLDataType(columnDef.columnType)
          StructField(
            name = columnDef.columnName,
            dataType = newType,
            nullable = true,
            metadata = formatString match {
              case None => Metadata.empty
              case Some(format) => dateFormats.put(columnDef.columnName, format)
                new MetadataBuilder().putString(SparkSqlDateTimeUtils.DATE_METADATA_KEY, format).build()
            }
          )
        }
      }
    )
    (schema, dateFormats)
  }

  /**
    * Converts from a Spark SQL schema to the Alpine 'TabularSchema' type. The 'TabularSchema'
    * object this method returns can be used to create any of the tabular Alpine IO types
    * (HDFSTabular dataset, dataTable etc.)
    * @param schema -a Spark SQL DataFrame schema
    * @return the equivalent Alpine schema for that dataset
    */
  def convertSparkSQLSchemaToTabularSchema(schema: StructType): TabularSchema = {
    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()

    val schemaItr = schema.iterator
    while (schemaItr.hasNext) {
      val colInfo = schemaItr.next()
      val colDef = ColumnDef(
        columnName = colInfo.name,
        columnType = convertSparkSQLDataTypeToColumnType(colInfo.dataType)
      )

      columnDefs += colDef
    }

    TabularSchema(columnDefs)
  }

  //this one adjusts the dates
  def getDateFormatFromSchema(schema: StructType) = {
    val dateFormats = mutable.Map[String, String]()

    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()

    val schemaItr = schema.iterator
    while (schemaItr.hasNext) {
      val colInfo = schemaItr.next()
      val colDef = convertSparkSQLDataTypeToColumnType(colInfo)
      if (colDef.columnType.format.isDefined && colDef.columnType.name == ColumnType.DateTime.name)
        dateFormats.put(colDef.columnName, colDef.columnType.format.get)

      columnDefs += colDef
    }

    TabularSchema(columnDefs)
  }

  def getDateMap(tabularSchema: TabularSchema) = {

    val dateFormats = mutable.Map[String, String]()

    tabularSchema.getDefinedColumns.foreach {
      columnDef => {
        val formatString = columnDef.columnType.format
        if (formatString.isDefined && columnDef.columnType.name == ColumnType.DateTime.name)
          dateFormats.put(columnDef.columnName, formatString.get)
      }
    }
    dateFormats
  }
}


object SparkSqlDateTimeUtils {

  val DATE_METADATA_KEY = "dateFormat"
  val SPARK_SQL_DATE_FORMAT = "yyyy-mm-dd"
  val SPARK_SQL_TIME_STAMP_FORMAT = "yyyy-mm-dd hh:mm:ss"

  /**
    * Get the custom date format for this field.
    */
  def getDatFormatInfo(field: StructField): Option[String] = {
    if (field.metadata.contains(DATE_METADATA_KEY))
      Some(field.metadata.getString(DATE_METADATA_KEY))
    else
      None
  }

  /**
    * Add a custom date format to a column defintion so that the date will be reformated on save
    */
  def addDateFormatInfo(field: StructField, format: String) = {
    StructField(
      name = field.name,
      dataType = field.dataType,
      nullable = field.nullable,
      metadata = new MetadataBuilder().putString(DATE_METADATA_KEY, format).build()
    )
  }
}

case class SparkSchemaUtilsClass extends SparkSchemaUtils


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
  /**
    * Convert from Alpine's 'ColumnType' to the corresponding Spark Sql Typ.
    * DateTime Behavior: Converts all DateTime columns to TimeStampType. If there is a format string
    * will add that to the metadata. If there is no format string, will use the ISO format
    * ("yyyy-mm-dd hh:mm:ss")
    */
  def convertColumnTypeToSparkSQLDataType(columnType: ColumnType.TypeValue, keepDatesAsStrings: Boolean): (DataType, Option[String]) = {
    columnType.name match {
      case ColumnType.Int.name => (IntegerType, None)
      case ColumnType.Long.name => (LongType, None)
      case ColumnType.Float.name => (FloatType, None)
      case ColumnType.Double.name => (DoubleType, None)
      case ColumnType.String.name => (StringType, None)
      case ColumnType.Boolean.name => (BooleanType, None)
      case ColumnType.DateTime.name =>
        val formatStr = columnType.format match {
          case Some(format: String) => columnType.format
          case None => Some(ColumnType.PIG_DATE_FORMAT) //enabled use with pig operators
        }
        val dataType = if (keepDatesAsStrings) StringType else TimestampType

        (dataType, formatStr)

      case _ => (StringType, columnType.format)
      // Rather than throwing an exception, treat it as a string type.
      // throw new UnsupportedOperationException(columnType.toString + " is not supported.")
    }
  }

  @deprecated("Will not properly handle data formats. Use toStructField")
  def convertColumnTypeToSparkSQLDataType(columnType: ColumnType.TypeValue): (DataType) = {
    convertColumnTypeToSparkSQLDataType(columnType, keepDatesAsStrings = false)._1
  }


  /**
    * Converts from a Spark SQL data type to an Alpine-specific ColumnType
    */
  @deprecated("This doesn't properly handle date formats. Use convertColumnTypeToSparkSQLDataType instead")
  def convertSparkSQLDataTypeToColumnType(dataType: DataType): ColumnType.TypeValue = {
    dataType match {
      case IntegerType => ColumnType.Int
      case LongType => ColumnType.Long
      case FloatType => ColumnType.Float
      case DoubleType => ColumnType.Double
      case StringType => ColumnType.String
      case DateType => ColumnType.DateTime
      case BooleanType => ColumnType.Boolean
      case TimestampType => ColumnType.DateTime
      case _ =>
        throw new UnsupportedOperationException(
          "Spark SQL data type " + dataType.toString + " is not supported."
        )
    }
  }


  /**
    * Converts from a Spark SQL Structfield  to an Alpine-specific ColumnDef.
    */
  def convertSparkSQLDataTypeToColumnType(structField: StructField): ColumnDef = {
    val name = structField.name
    val dataType: DataType = structField.dataType
    dataType match {
      case IntegerType => ColumnDef(name, ColumnType.Int)
      case LongType => ColumnDef(name, ColumnType.Long)
      case FloatType => ColumnDef(name, ColumnType.Float)
      case DoubleType => ColumnDef(name, ColumnType.Double)
      case StringType => ColumnDef(name, ColumnType.String)
      case BooleanType => ColumnDef(name, ColumnType.Boolean)
      case DateType =>
        ColumnDef(name, ColumnType.DateTime(
          SparkSqlDateTimeUtils.getDatFormatInfo(structField)
            .getOrElse(ColumnType.PIG_DATE_FORMAT)))
      case TimestampType =>
        ColumnDef(name, ColumnType.DateTime(
          SparkSqlDateTimeUtils.getDatFormatInfo(structField)
            .getOrElse(ColumnType.PIG_DATE_FORMAT)))
      case _ =>
        throw new UnsupportedOperationException(
          "Spark SQL data type " + dataType.toString + " is not supported."
        )
    }
  }

  /**
    * Convert the Alpine 'TabularSchema' with column names and types to the equivalent Spark SQL
    * data frame header.
    *
    * Date/Time behavior:
    * The same as convertTabularSchemaToSparkSQLSchema(tabularSchema, false). Will NOT convert special date
    * formats to String. Instead will render Alpine date formats as Spark SQL TimeStampType. The original date
    * format will be stored as metadata in the StructFiled object for that column definition.
    *
    * @param tabularSchema An Alpine 'TabularSchemaOutline' object with fixed column definitions
    *                      containing a name and Alpine specific type.
    * @return
    */
  def convertTabularSchemaToSparkSQLSchema(tabularSchema: TabularSchema): StructType = {
    val dateFormats = mutable.Map[String, String]()
    val schema = StructType(
      tabularSchema.getDefinedColumns.map {
        columnDef => {

          val (newType, formatString) = convertColumnTypeToSparkSQLDataType(columnDef.columnType, keepDatesAsStrings = false)
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
    schema
  }

  def convertTabularSchemaToSparkSQLSchema(tabularSchema: TabularSchema, keepDatesAsStrings: Boolean): StructType = {
    StructType(
      tabularSchema.getDefinedColumns.map {
        columnDef => {
          convertToStructField(columnDef, nullable = true, keepDatesAsStrings)
        }
      }
    )
  }

  /**
    * Converts from a Spark SQL schema to the Alpine 'TabularSchema' type. The 'TabularSchema'
    * object this method returns can be used to create any of the tabular Alpine IO types
    * (HDFSTabular dataset, dataTable etc.)
    *
    * Date format behavior: If the column def has not metadata stored at the DATE_METADATA_KEY constant,
    * it wll convert DateType objects to ColumnType(DateTime, "yyyy-mm-dd") and
    * TimeStampType objects to ColumnType(DateTime, "yyyy-mm-dd hh:mm:ss")
    * otherwise will create a column type of ColumnType(DateTime, custom_date_format) where
    * custom_date_format is whatever date format was specified by the column metadata.
    *
    * @param schema -a Spark SQL DataFrame schema
    * @return the equivalent Alpine schema for that dataset
    */
  def convertSparkSQLSchemaToTabularSchema(schema: StructType): TabularSchema = {

    val columnDefs = new mutable.ArrayBuffer[ColumnDef]()

    val schemaItr = schema.iterator
    while (schemaItr.hasNext) {
      val colInfo = schemaItr.next()
      val colDef = convertSparkSQLDataTypeToColumnType(colInfo)
      columnDefs += colDef
    }

    TabularSchema(columnDefs)
  }

  //this one adjusts the dates

  def getDateMap(tabularSchema: TabularSchema): Map[String, String] = {

    val dateFormats = mutable.Map[String, String]()

    tabularSchema.getDefinedColumns.foreach {
      columnDef => {
        val formatString = columnDef.columnType.format
        if (formatString.isDefined && columnDef.columnType.name == ColumnType.DateTime.name)
          dateFormats.put(columnDef.columnName, formatString.get)
      }
    }
    dateFormats.toMap
  }

  def toStructField(columnDef: ColumnDef, nullable: Boolean = true): StructField = {
    convertToStructField(columnDef, nullable, keepDatesAsStrings = false)
  }

  private def convertToStructField(columnDef: ColumnDef, nullable: Boolean, keepDatesAsStrings: Boolean): StructField = {
    val (_, format) = convertColumnTypeToSparkSQLDataType(columnDef.columnType, keepDatesAsStrings)
    val newDef = StructField(
      columnDef.columnName,
      convertColumnTypeToSparkSQLDataType(columnDef.columnType, keepDatesAsStrings)._1,
      nullable = nullable
    )
    format match {
      case Some(formatStr) => SparkSqlDateTimeUtils.addDateFormatInfo(newDef, formatStr)
      case None => newDef
    }
  }
}


object SparkSqlDateTimeUtils {

  val DATE_METADATA_KEY = "dateFormat"

  /**
    * Get the custom date format for this Column definition.
    */
  def getDatFormatInfo(field: StructField): Option[String] = {
    if (field.metadata.contains(DATE_METADATA_KEY))
      Some(field.metadata.getString(DATE_METADATA_KEY))
    else
      None
  }

  /**
    * Add a custom date format to a column definition so that the date will be re-formatted by the
    * 'saveDataFrame method.
    */
  def addDateFormatInfo(field: StructField, format: String): StructField = {
    StructField(
      name = field.name,
      dataType = field.dataType,
      nullable = field.nullable,
      metadata = new MetadataBuilder().putString(DATE_METADATA_KEY, format).build()
    )
  }
}

case object SparkSchemaUtils extends SparkSchemaUtils


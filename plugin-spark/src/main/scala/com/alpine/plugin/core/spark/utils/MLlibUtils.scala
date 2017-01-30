/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */
package com.alpine.plugin.core.spark.utils

import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructType}

/**
  * Helper functions that are useful for working with MLlib.
  */
object MLlibUtils {

  /**
    *
    * @param dependentColumnIndex     Index of the dependent column in the column set.
    * @param independentColumnIndices Indices of the independent columns.
    * @return Function mapping a Row of the correct schema to a labelled point.
    */
  def toLabeledPoint(dependentColumnIndex: Int, independentColumnIndices: Seq[Int]): (Row) => LabeledPoint = {
    row =>
      val numColumns: Int = independentColumnIndices.length
      val independentValues: Array[Double] = Array.ofDim(numColumns)
      var i = 0
      while (i < numColumns) {
        independentValues(i) = anyToDouble(row(independentColumnIndices(i)))
        i += 1
      }
      new LabeledPoint(
        anyToDouble(row.get(dependentColumnIndex)),
        new DenseVector(independentValues)
      )
  }

  /**
    * Converts input of type Any to Double.
    * Does this by casting to java.lang.Number,
    * and then taking the double value.
    *
    * Will return Double.NaN in the case of bad input.
    *
    * @param a input to be converted to Double.
    * @return Double representation of the number, or Double.NaN if impossible.
    */
  def anyToDouble(a: Any): Double = {
    try {
      a.asInstanceOf[Number].doubleValue()
    }
    catch {
      case _: NullPointerException => Double.NaN
      case _: ClassCastException => Double.NaN
    }
  }


  def rowToDoubleVector(row: sql.Row): Vector = {
    val rowAsDoubles: Array[Double] = row.toSeq.map(x => MLlibUtils.anyToDouble(x)).toArray
    Vectors.dense(rowAsDoubles)
  }

  /**
    * An ugly function that handles the case where data that was a long, has been converted to a double
    * and then can't be converted back i.e. 10 becomes 10.0 and then creating a dataFrame with an integer
    * or long column fails. A non numeric input in a double or integer column will be converted to null,
    * since round of Double.NAN is 0. Set the value of the nullable parameter to true in order to keep the
    * behavior where null values in Integer and Long columns are converted to zeros.
    *
    * @param rowRDD and rdd of some sequence type that will be the body of the DataFrame
    * @param schema the schema that we want tthe new RDD to conform to
    * @return rowRDD with each value parsed so it won't cause failures when
    *         sqlContext.createDataFrame(rowRDD, schema) is called
    */
  def mapSeqToCorrectType[T <: Seq[Any]](rowRDD: RDD[T], schema: StructType, nullable: Boolean = true): RDD[Row] = {
    val schemaLength = schema.length
    val typeArray = schema.fields.map(field => field.dataType)
    rowRDD.map(rowSeq => {
      require(rowSeq.length == schemaLength)
      val seq = rowSeq.zipWithIndex.map { case (v, index) =>
        val sqlType: DataType = typeArray(index)
        sqlType match {

          case IntegerType =>
            val double = anyToDouble(v)
            //SparkSQL converts Double.NAN to zero. Instead of doing that, I will use null
            if (nullable && double.isNaN) null
            else double.round.toInt

          case LongType =>
            //SparkSQL converts Double.NAN to zero. Instead of doing that, I will use null
            val double = anyToDouble(v)
            if (nullable && double.isNaN) null
            else double.round

          case DoubleType => anyToDouble(v)

          case (_) => v
        }
      }
      Row.fromSeq(seq)
    })
  }
}

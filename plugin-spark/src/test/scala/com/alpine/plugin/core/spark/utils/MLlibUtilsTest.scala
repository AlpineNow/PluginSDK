/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.plugin.core.spark.utils

import com.alpine.plugin.core.spark.utils.MLlibUtils._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite


class MLlibUtilsTest extends FunSuite {

  test("testToLabeledPoint") {
    val toLabeledPointFunc = toLabeledPoint(dependentColumnIndex = 2 , independentColumnIndices = Array(1, 3))
    assert(LabeledPoint(2.5, new DenseVector(Array(2.0, 3.0))) === toLabeledPointFunc(Row.fromTuple(null, 2, 2.5, 3)))
    assert(LabeledPoint(2.5, new DenseVector(Array(2.0, 3.0))) === toLabeledPointFunc(Row.fromTuple(3.2, 2L, 2.5F, 3L)))
  }

  test("testAnyToDouble") {
    assert(1.0 === anyToDouble(1L))
    assert(1.0 === anyToDouble(1d))
    assert(1.0 === anyToDouble(1D))
    assert(1.0 === anyToDouble(1F))
    assert(anyToDouble(null).isNaN)
    assert(anyToDouble("Help").isNaN)
  }

  test("mapSeqToCorrectType") {
    import TestSparkContexts._

    val schema = StructType(Array(
      StructField("StringType", StringType),
      StructField("IntType", IntegerType),
      StructField("LongType", LongType),
      StructField("DoubleType", DoubleType)))

    val sequence = Seq(
      Seq("thing1", 5.0, 4.0, 3.3),
      Seq("thing2", "badValue", "badValue", "badValue"),
      Seq("thing3", null, null, null)
    )

    val expectedRow = Set(
      Row("thing1", 5, 4L, 3.3).mkString(" "),
      Row("thing2", null, null, Double.NaN).mkString(" "),
      Row("thing3", null, null, Double.NaN).mkString(" ")
    )

    val parallelSequence = sc.parallelize(sequence)
    val newRDD = MLlibUtils.mapSeqToCorrectType(parallelSequence, schema)
    val newDF = sqlContext.createDataFrame(newRDD, schema)
    val result = newDF.collect().map(_.mkString(" "))
    assert(result.length == sequence.length)
    assert(result.toSet == expectedRow)
  }

}

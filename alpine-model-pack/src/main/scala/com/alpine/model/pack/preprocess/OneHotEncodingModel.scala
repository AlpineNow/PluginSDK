/*
 * Copyright (C) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.preprocess

import com.alpine.model.RowModel
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.ColumnarSQLExpression

/**
 * Model to apply one-hot encoding to categorical input features.
 * Result will be a sequence of 1s and 0s.
 */
case class OneHotEncodingModel(oneHotEncodedFeatures: Seq[OneHotEncodedFeature], inputFeatures: Seq[ColumnDef],  override val identifier: String = "") extends RowModel {

  override def transformer = OneHotEncodingTransformer(oneHotEncodedFeatures)

  def outputFeatures: Seq[ColumnDef] = {
    inputFeatures.indices.flatMap(i => {
      val p = oneHotEncodedFeatures(i)
      p.hotValues.indices.map(j => new ColumnDef(inputFeatures(i).columnName + "_" + j, ColumnType.Int))
    })
  }

  override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(OneHotEncodingSQLTransformer(this, sqlGenerator))
}

/**
 * One hot encoding for a single feature.
 * The baseValue is encoded as all 0s to ensure a linear independence of the range.
 *
 * @param hotValues values to be encoded as 1 at the corresponding index, 0s elsewhere.
 * @param baseValue value to be encoded as a vector as 0s.
 */
case class OneHotEncodedFeature(hotValues: Seq[String], baseValue: Option[String]) {
  def getScorer = SingleOneHotEncoder(this)
}

object OneHotEncodedFeature {
  def apply(hotValues: Seq[String], baseValue: String): OneHotEncodedFeature = {
    OneHotEncodedFeature(hotValues, Some(baseValue))
  }
}

case class OneHotEncodingTransformer(pivotsWithFeatures: Seq[OneHotEncodedFeature]) extends Transformer {

  // Use toArray for indexing efficiency.
  private val scorers = pivotsWithFeatures.map(x => x.getScorer).toArray

  lazy val outputDim: Int = pivotsWithFeatures.map(t => t.hotValues.size).sum

  override def apply(row: Row): Row = {
    val output = Array.ofDim[Any](outputDim)
    var inputIndex = 0
    var outputIndex = 0
    while (inputIndex < scorers.length) {
      outputIndex = scorers(inputIndex).setFeatures(output, row(inputIndex), outputIndex)
      inputIndex += 1
    }
    output
  }
}

/**
 * Applies One-hot encoding for a single feature.
 * e.g. if
 * hotValues = Seq["apple", "raspberry"]
 * baseValue = "orange"
 *
 * apply("apple") = [1,0]
 * apply("raspberry") = [0,1]
 * apply("orange") = [0,0]
 *
 * apply("banana") throws exception "banana is an unrecognised value".
 *
 * @param transform case class wrapping hot values and base values.
 */
case class SingleOneHotEncoder(transform: OneHotEncodedFeature) {
  @transient lazy val hotValuesArray = transform.hotValues.toArray
  @transient lazy val resultDimension = transform.hotValues.length
  def setFeatures(currentRow: Array[Any], value: Any, startingIndex: Int): Int = {
    if (startingIndex + resultDimension > currentRow.length) {
      throw new Exception("Cannot do this!!")
    } else {
      var found = false
      var i = 0
      while (i < resultDimension) {
        currentRow(startingIndex + i) = if (hotValuesArray(i).equals(value)) {
          found = true
          1
        } else {
          0
        }
        i += 1
      }
      if (!found && (transform.baseValue.isEmpty || !transform.baseValue.get.equals(value))) {
        // TODO: Error handling.
        throw new Exception(s"""$value is an unrecognised value""")
      }
      startingIndex + i
    }
  }
}

case class OneHotEncodingSQLTransformer(val model: OneHotEncodingModel, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {

  override def getSQLExpressions = {
    // TODO: Bad data handling.
    (inputColumnNames zip model.oneHotEncodedFeatures).flatMap {
      case (name, oneHotEncodedFeatures) =>
        val inputFeature: String = name.escape(sqlGenerator)
        oneHotEncodedFeatures.hotValues.map(v => {
          val sql = s"""(CASE WHEN ($inputFeature = '$v') THEN 1 ELSE 0 END)"""
          ColumnarSQLExpression(sql)
        })
    }
  }

}

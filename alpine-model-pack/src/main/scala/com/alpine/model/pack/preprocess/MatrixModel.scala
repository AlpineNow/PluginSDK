package com.alpine.model.pack.preprocess

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.modelconverters.MatrixPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.sql.SimpleSQLTransformer
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.Transformer
import com.alpine.transformer.sql.{ColumnarSQLExpression, SQLTransformer}

/**
  * Created by Jennifer Thompson on 3/17/16.
  */
case class MatrixModel(values: Seq[Seq[java.lang.Double]], inputFeatures: Seq[ColumnDef], override val identifier: String = "")
  extends RowModel with PFAConvertible {
  override def transformer: Transformer = MatrixTransformer(this)

  override def sqlTransformer(sqlGenerator: SQLGenerator): Option[SQLTransformer] = Some(new MatrixSQLTransformer(this, sqlGenerator))

  def outputFeatures = {
    MatrixModel.generateOutputFeatures(values.indices)
  }

  override def getPFAConverter: PFAConverter = new MatrixPFAConverter(this)
}

object MatrixModel {
  def generateOutputFeatures(indices: Seq[Int]): Seq[ColumnDef] = {
    indices.map(i => ColumnDef("y_" + i, ColumnType.Double))
  }
}

case class MatrixTransformer(model: MatrixModel) extends Transformer {

  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)

  private val values: Array[Array[Double]] = model.values.map(v => TransformerUtil.javaDoubleSeqToArray(v)).toArray

  override def apply(row: Row): Row = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    val result = Array.ofDim[Double](values.length)
    var i = 0
    while (i < values.length) {
      var x = 0d
      var j = 0
      while (j < rowAsDoubleArray.length) {
        x += (rowAsDoubleArray(j) * values(i)(j))
        j += 1
      }
      result(i) = x
      i += 1
    }
    result
  }
}

case class MatrixSQLTransformer(model: MatrixModel, sqlGenerator: SQLGenerator) extends SimpleSQLTransformer {
  override def getSQLExpressions: Seq[ColumnarSQLExpression] = {
    model.values.map(doubles => {
      val expression = (doubles zip inputColumnNames).flatMap {
        case (value, name) =>
          if (value == 0) {
            None
          } else {
            Some(s"${name.escape(sqlGenerator)} * $value")
          }
      }.mkString(" + ")
      if (expression != "") {
        expression
      } else {
        // 0 is the identity for addition.
        "0"
      }
    }
    ).map(ColumnarSQLExpression)
  }

}


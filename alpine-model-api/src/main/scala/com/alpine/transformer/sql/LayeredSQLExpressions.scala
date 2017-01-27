package com.alpine.transformer.sql

import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.util.SQLUtility

case class LayeredSQLExpressions(layers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]])

case class LayeredSQLExpression(finalLayer: ColumnarSQLExpression, intermediateLayers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]] = Nil)

case class ClassificationModelSQLExpressions(labelColumnSQL: ColumnarSQLExpression,
                                             confidenceSQL: Map[String, ColumnarSQLExpression],
                                             intermediateLayers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]] = Nil)

object ClassificationModelSQLExpressions {

  def apply(labelValuesToColumnNames: Seq[(String, ColumnName)],
            layers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]],
            sqlGenerator: SQLGenerator): ClassificationModelSQLExpressions = {
    /**
      * The predicted label is the one with the highest confidence value.
      */
    val labelSQL = ColumnarSQLExpression(SQLUtility.argMaxSQL(labelValuesToColumnNames, sqlGenerator))

    ClassificationModelSQLExpressions(
      labelSQL,
      labelValuesToColumnNames.map {
        case (labelValue, columnName) => (labelValue, columnName.asColumnarSQLExpression(sqlGenerator))
      }.toMap,
      layers
    )
  }

}

case class ClusteringModelSQLExpressions(labelColumnSQL: ColumnarSQLExpression,
                                             distanceSQL: Map[String, ColumnarSQLExpression],
                                             intermediateLayers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]] = Nil)

object ClusteringModelSQLExpressions {

  def apply(labelValuesToColumnNames: Seq[(String, ColumnName)],
            layers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]],
            sqlGenerator: SQLGenerator): ClusteringModelSQLExpressions = {
    /**
      * The predicted label is the one with the smallest distance value.
      */
    val labelSQL = ColumnarSQLExpression(SQLUtility.argMinSQL(labelValuesToColumnNames, sqlGenerator))

    ClusteringModelSQLExpressions(
      labelSQL,
      labelValuesToColumnNames.map {
        case (labelValue, columnName) => (labelValue, columnName.asColumnarSQLExpression(sqlGenerator))
      }.toMap,
      layers
    )
  }

}

case class RegressionModelSQLExpression(predictionColumnSQL: ColumnarSQLExpression,
                                        intermediateLayers: Seq[Seq[(ColumnarSQLExpression, ColumnName)]] = Nil)

case class ColumnarSQLExpression(sql: String)

case class ColumnName(rawName: String) {

  def this(columnDef: ColumnDef) {
    this(columnDef.columnName)
  }

  def escape(sqlGenerator: SQLGenerator) = sqlGenerator.quoteIdentifier(rawName)

  def asColumnarSQLExpression(sqlGenerator: SQLGenerator) = ColumnarSQLExpression(this.escape(sqlGenerator))
}

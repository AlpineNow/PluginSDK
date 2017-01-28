package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.multiple.{CombinerModel, GroupByRegressionModel}
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.sql.{ColumnarSQLExpression, RegressionModelSQLExpression, RegressionSQLTransformer}
import com.alpine.util.SQLUtility

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class GroupByRegressionSQLTransformer(val model: GroupByRegressionModel, val sqlGenerator: SQLGenerator) extends RegressionSQLTransformer {

  override def getPredictionSQL: RegressionModelSQLExpression = {
    val subModelsSeq = model.modelsByGroup.toSeq
    val groupByColumnModel = UnitModel(Seq(model.groupByFeature))

    val combinerModel = CombinerModel.make(subModelsSeq.map(_._2) ++ Seq(groupByColumnModel))
    /**
      * TODO:
      * We could make this more efficient by consolidating duplicate expressions,
      * but this will work for now.
      */
    val combinedSQL = combinerModel.sqlTransformer(sqlGenerator).get.getSQL
    val groupBySQLExpression = combinedSQL.layers.last.last._2.asColumnarSQLExpression(sqlGenerator)
    val finalValueSQL = SQLUtility.groupBySQL(
      groupBySQLExpression,
      (subModelsSeq.map {
        case (groupValue, _) => ColumnarSQLExpression(SQLUtility.wrapAsValue(groupValue))
      } zip combinedSQL.layers.last.map(_._2.asColumnarSQLExpression(sqlGenerator))).toMap
    )
    RegressionModelSQLExpression(finalValueSQL, combinedSQL.layers)
  }

}

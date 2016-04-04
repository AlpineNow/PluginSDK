/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.multiple.CombinerModel
import com.alpine.model.pack.util.SQLModifyUtil
import com.alpine.sql.{AliasGenerator, SQLGenerator}
import com.alpine.transformer.sql.{ColumnName, LayeredSQLExpressions, SQLTransformer}

import scala.collection.mutable

case class CombinerSQLTransformer(model: CombinerModel, sqlGenerator: SQLGenerator) extends SQLTransformer {

  private val transformers = model.models.map(_.model.sqlTransformer(sqlGenerator).get)

  override def getSQL: LayeredSQLExpressions = {
    val sqls = transformers.map(t => t.getSQL)
    val combinedSQL = CombinerSQLTransformer.combineLayeredSQLExpressions(sqlGenerator, sqls)
    LayeredSQLExpressions(
      combinedSQL.layers.take(combinedSQL.layers.length - 1) ++
        Seq((combinedSQL.layers.last zip outputColumnNames).map {
          case ((expression, dummyColumnName), realColumnName) => (expression, realColumnName)
        })
    )
  }

}

object CombinerSQLTransformer {
  def combineLayeredSQLExpressions(sqlGenerator: SQLGenerator, sqls: Seq[LayeredSQLExpressions]): LayeredSQLExpressions = {
    val maxDepth = sqls.map(s => s.layers.length).max
    val aliasGenerator = new AliasGenerator("column")
    val paddedSQL: Seq[LayeredSQLExpressions] = sqls.map(s => {
      val padding = maxDepth - s.layers.length
      if (padding == 0) {
        s
      } else {
        val nextLayer = s.layers.last
        val aliases = nextLayer.map(f => {
          ColumnName(aliasGenerator.getNextAlias)
        })
        val paddingLayer = aliases.map(e => (e.asColumnarSQLExpression(sqlGenerator), e))

        LayeredSQLExpressions(s.layers.take(s.layers.length - 1)
          ++ Seq(s.layers.last.map { case (expression, name) => expression } zip aliases)
          ++ Range(0, padding).map(i => paddingLayer)
        )
      }
    })

    // Could replace this with a fold to avoid mutability, but I think that would make it even harder to read.
    val columnNamesByLayer = Range(0, maxDepth).map(i => mutable.Set[ColumnName]())
    val renamedSQL = paddedSQL.map(s => {
      var aliasMap: Option[Map[ColumnName, ColumnName]] = None
      s.layers.zipWithIndex.map {
        case (layer, layerIndex) =>
          val columnNamesForThisLayer = columnNamesByLayer(layerIndex)
          val originalColumnNames = layer.map { case (expression, columnName) => columnName }

          // Replace the column names in the expressions using the map from the last layer.
          val newExpressions = if (aliasMap.nonEmpty) {
            layer.map {
              case (expression, name) => SQLModifyUtil.replaceColumnNames(expression, aliasMap.get, sqlGenerator)
            }
          } else {
            layer.map {
              case (expression, name) => expression
            }
          }

          val nameConflicts = columnNamesForThisLayer intersect originalColumnNames.toSet
          if (nameConflicts.isEmpty) {
            columnNamesForThisLayer ++= originalColumnNames
            aliasMap = None
            newExpressions zip originalColumnNames
          } else {
            val columnNamesToAvoid = columnNamesForThisLayer union originalColumnNames.toSet
            aliasMap = Some(nameConflicts.map(n => (n, {
              var alias = ColumnName(aliasGenerator.getNextAlias)
              while (columnNamesToAvoid.contains(alias)) {
                alias = ColumnName(aliasGenerator.getNextAlias)
              }
              alias
            })).toMap)
            columnNamesForThisLayer ++= aliasMap.get.values

            val newNames = originalColumnNames.map(x => aliasMap.get.get(x) match {
              case Some(name) => name
              case None => x
            })

            newExpressions zip newNames
          }
      }
    })

    LayeredSQLExpressions(Range(0, maxDepth).map(i => renamedSQL.flatMap(s => s(i))))
  }

  def make(model: CombinerModel, sqlGenerator: SQLGenerator): Option[CombinerSQLTransformer] = {
    val sqlTransformers = model.models.map(_.model.sqlTransformer(sqlGenerator))
    val canBeScoredInSQL = sqlTransformers.forall(_.isDefined)
    if (canBeScoredInSQL) {
      Some(new CombinerSQLTransformer(model, sqlGenerator))
    } else {
      None
    }
  }

}

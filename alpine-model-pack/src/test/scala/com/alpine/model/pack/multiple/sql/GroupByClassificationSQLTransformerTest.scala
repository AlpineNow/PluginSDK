package com.alpine.model.pack.multiple.sql

import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.model.pack.multiple.GroupByClassificationModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.sql.AliasGenerator
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/17/16.
  */
class GroupByClassificationSQLTransformerTest extends FunSuite {

  val modelA = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
    "yes",
    Seq(0.5, -0.5).map(java.lang.Double.valueOf), 1.0)),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
  )

  val modelB = new MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
    "yes",
    Seq(0.1).map(java.lang.Double.valueOf), -10)),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long))
  )

  val groupModel = new GroupByClassificationModel(ColumnDef("wind", ColumnType.String), Map("true" -> modelA, "false" -> modelB))

  val simpleSQLGenerator: SimpleSQLGenerator = new SimpleSQLGenerator()

  test("Should generate the correct SQL") {
    val sql = groupModel.sqlTransformer(simpleSQLGenerator).get.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sql, "\"demo\".\"golfnew\"", new AliasGenerator(), simpleSQLGenerator)
    val expectedSQL =
      """SELECT (CASE WHEN ("CONF0" > "CONF1")
        |  THEN 'yes'
        |        ELSE 'no' END) AS "PRED"
        |FROM (SELECT
        |        (CASE WHEN ("wind" = 'true')
        |          THEN "CONF0"
        |         WHEN ("wind" = 'false')
        |           THEN "CONF0_1"
        |         ELSE NULL END) AS "CONF0",
        |        (CASE WHEN ("wind" = 'true')
        |          THEN "CONF1"
        |         WHEN ("wind" = 'false')
        |           THEN "CONF1_1"
        |         ELSE NULL END) AS "CONF1"
        |      FROM (SELECT
        |              "ce0"      AS "CONF0",
        |              "baseVal"  AS "CONF1",
        |              "column_4" AS "CONF0_1",
        |              "column_5" AS "CONF1_1",
        |              "column_0" AS "wind"
        |            FROM (SELECT
        |                    1 / "sum"               AS "baseVal",
        |                    "e0" / "sum"            AS "ce0",
        |                    1 / "column_2"          AS "column_5",
        |                    "column_3" / "column_2" AS "column_4",
        |                    "column_0"              AS "column_0"
        |                  FROM (SELECT
        |                          1 + "e0"       AS "sum",
        |                          "e0"           AS "e0",
        |                          1 + "column_1" AS "column_2",
        |                          "column_1"     AS "column_3",
        |                          "column_0"     AS "column_0"
        |                        FROM (SELECT
        |                                EXP(1.0 + "temperature" * 0.5 + "humidity" * -0.5) AS "e0",
        |                                EXP(-10.0 + "temperature" * 0.1)                   AS "column_1",
        |                                "wind"                                             AS "column_0"
        |                              FROM "demo"."golfnew") AS alias_0) AS alias_1) AS alias_2) AS alias_3) AS alias_4"""
        .stripMargin.replaceAll("\\s+", " ").replaceAllLiterally("\n", "")

    assert(expectedSQL === selectStatement)
  }

}

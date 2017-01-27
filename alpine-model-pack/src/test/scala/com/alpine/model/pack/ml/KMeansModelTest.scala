/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.result.ClusteringResult
import com.alpine.sql.AliasGenerator
import com.alpine.transformer.sql.{ColumnName, ColumnarSQLExpression, ClusteringModelSQLExpressions}
import com.alpine.util.{SQLUtility, SimpleSQLGenerator}
import org.scalatest.FunSuite

/**
 * Tests serialization and scoring of KMeansModelTest.
 */
class KMeansModelTest extends FunSuite {

  val clusters = Seq(ClusterInfo("A", Seq(231.5/3, 239.0/3)), ClusterInfo("B", Seq(68.25, 68.75)), ClusterInfo("C", Seq(73.5,92.75)))
  val inputFeatures = Seq(ColumnDef("humidity", ColumnType.Double), ColumnDef("temperature", ColumnType.Double))
  val model = KMeansModel(clusters, inputFeatures, "KM")

  test("Should serialize correctly.") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should score correctly") {
    val scorer = model.transformer
    val expectedResult = ClusteringResult(Seq("A", "B", "C"), Array(9.476,23.33,13.867))
    assert(expectedResult.equals(scorer.score(Seq(85,85)), 1e-2))
  }

  test("Should generate the correct SQL Expression") {
    val clusteringSQL = model.sqlTransformer(new SimpleSQLGenerator).get.getClusteringSQL
    val expectedClusteringSQL = ClusteringModelSQLExpressions(
      ColumnarSQLExpression( s"""CASE WHEN "DIST_0" IS NULL OR "DIST_1" IS NULL OR "DIST_2" IS NULL THEN NULL ELSE (CASE WHEN ("DIST_0" < "DIST_1" AND "DIST_0" < "DIST_2") THEN 'A' WHEN ("DIST_1" < "DIST_2") THEN 'B' ELSE 'C' END) END"""),
      Map("A" -> ColumnarSQLExpression(""""DIST_0""""), "B" -> ColumnarSQLExpression(""""DIST_1""""), "C" -> ColumnarSQLExpression(""""DIST_2"""")),
      List(
        List(
          (ColumnarSQLExpression( """SQRT((77.16666666666667 - "humidity") * (77.16666666666667 - "humidity") + (79.66666666666667 - "temperature") * (79.66666666666667 - "temperature"))"""), ColumnName("DIST_0")),
          (ColumnarSQLExpression( """SQRT((68.25 - "humidity") * (68.25 - "humidity") + (68.75 - "temperature") * (68.75 - "temperature"))"""), ColumnName("DIST_1")),
          (ColumnarSQLExpression( """SQRT((73.5 - "humidity") * (73.5 - "humidity") + (92.75 - "temperature") * (92.75 - "temperature"))"""), ColumnName("DIST_2"))
        )
      )
    )

    assert(expectedClusteringSQL === clusteringSQL)
  }

  test("Should generate the correct table generation SQL") {
    val sqlGenerator = new SimpleSQLGenerator
    val sqlExpressions = model.sqlTransformer(sqlGenerator).get.getSQL
    val sql = SQLUtility.createTable(sqlExpressions, """"demo"."golfnew"""", """"demo"."delete_me"""", new AliasGenerator, sqlGenerator)

    val expectedSQL =
      """
         |CREATE TABLE "demo"."delete_me"
         | AS SELECT
         | CASE WHEN "DIST_0" IS NULL OR "DIST_1" IS NULL OR "DIST_2" IS NULL THEN NULL ELSE
         | (CASE WHEN ("DIST_0" < "DIST_1" AND "DIST_0" < "DIST_2") THEN 'A' WHEN ("DIST_1" < "DIST_2") THEN 'B' ELSE 'C' END)
         | END
         | AS "PRED"
         | FROM
         | (SELECT
         | SQRT((77.16666666666667 - "humidity") * (77.16666666666667 - "humidity") + (79.66666666666667 - "temperature") * (79.66666666666667 - "temperature"))
         | AS "DIST_0",
         | SQRT((68.25 - "humidity") * (68.25 - "humidity") + (68.75 - "temperature") * (68.75 - "temperature"))
         | AS "DIST_1",
         | SQRT((73.5 - "humidity") * (73.5 - "humidity") + (92.75 - "temperature") * (92.75 - "temperature"))
         | AS "DIST_2"
         | FROM "demo"."golfnew"
         |) AS alias_0""".stripMargin.replace("\n", "")
    assert(expectedSQL === sql)
  }

}

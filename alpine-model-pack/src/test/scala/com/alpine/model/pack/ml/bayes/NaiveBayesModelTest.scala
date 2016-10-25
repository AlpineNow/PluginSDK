/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.model.pack.ml.bayes

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.transformer.sql.{ClassificationModelSQLExpressions, ColumnName, ColumnarSQLExpression}
import com.alpine.util.SimpleSQLGenerator
import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  * Created by Jennifer Thompson on 7/7/16.
  */
class NaiveBayesModelTest extends FunSuite {

  val inputFeatures = Seq(
    ColumnDef("outlook", ColumnType.String),
    ColumnDef("temperature", ColumnType.Long)
  )

  val distributions = Seq(
    Distribution(
      classLabel = "yes",
      priorProbability = 9,
      likelihoods = Seq(
        TypeWrapper(CategoricalLikelihood(
          Seq(
            CategoryCount("sunny", 2),
            CategoryCount("rain", 3),
            CategoryCount("overcast", 4)
          )
        )),
        TypeWrapper(GaussianLikelihood(73, 6.1644))
      )
    ),
    Distribution(
      classLabel = "no",
      priorProbability = 5,
      likelihoods = Seq(
        TypeWrapper(CategoricalLikelihood(
          Seq(
            CategoryCount("sunny", 3),
            CategoryCount("rain", 2),
            CategoryCount("overcast", 0)
          )
        )),
        TypeWrapper(GaussianLikelihood(74.6, 7.893))
      )
    )
  )

  val model = NaiveBayesModel(
    inputFeatures,
    "play",
    distributions,
    0.01
  )

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(model)
  }

  test("Should do in-memory prediction correctly") {
    val transformer = model.transformer
    transformer.score(Seq("sunny", 85)).toMap("yes") should be(0.2407 +- 1e-2)
    transformer.score(Seq("sunny", 69)).toMap("yes") should be(0.4797 +- 1e-2)
    transformer.score(Seq("rain", 68)).toMap("yes") should be(0.6651 +- 1e-2)
    transformer.score(Seq("overcast", 72)).toMap("yes") should be(0.9873 +- 1e-2)
  }

  test("Should generate correct SQL") {
    val sqlTransformer = model.sqlTransformer(new SimpleSQLGenerator).get
    val sql = sqlTransformer.getClassificationSQL
    val conf0SQL = ColumnarSQLExpression(""""conf_0"""")
    val conf1SQL = ColumnarSQLExpression(""""conf_1"""")
    val expectedClassificationSQL = ClassificationModelSQLExpressions(
      labelColumnSQL = ColumnarSQLExpression("""(CASE WHEN ("conf_0" > "conf_1") THEN 'yes' ELSE 'no' END)"""),
      confidenceSQL = Map("yes" -> conf0SQL, "no" -> ColumnarSQLExpression(""""conf_1"""")),
      intermediateLayers = List(
        List(
          (
            ColumnarSQLExpression(
              """9.0 *
                | (CASE WHEN "outlook" IS NULL THEN 1 WHEN "outlook" = 'sunny' THEN 0.2222222222222222 WHEN "outlook" = 'rain' THEN 0.3333333333333333 WHEN "outlook" = 'overcast' THEN 0.4444444444444444 ELSE 0.01 END) *
                | (CASE WHEN "temperature" IS NULL THEN 1 ELSE EXP(-("temperature" - 73.0)*("temperature" - 73.0) / 75.99965472) / 6.1644 END)""".stripMargin.replace("\n", "")
            ),
            ColumnName("conf_0")
            ),
          (ColumnarSQLExpression(
            """5.0 *
              | (CASE WHEN "outlook" IS NULL THEN 1 WHEN "outlook" = 'sunny' THEN 0.6 WHEN "outlook" = 'rain' THEN 0.4 WHEN "outlook" = 'overcast' THEN 0.0 ELSE 0.01 END) *
              | (CASE WHEN "temperature" IS NULL THEN 1 ELSE EXP(-("temperature" - 74.6)*("temperature" - 74.6) / 124.59889799999999) / 7.893 END)""".stripMargin.replace("\n", "")
          ), ColumnName("conf_1"))
        ),
        List(
          (conf0SQL, ColumnName("conf_0")),
          (conf1SQL, ColumnName("conf_1")),
          (ColumnarSQLExpression(""""conf_0" + "conf_1""""), ColumnName("sum"))
        ),
        List(
          (ColumnarSQLExpression(""""conf_0" / "sum""""), ColumnName("conf_0")),
          (ColumnarSQLExpression(""""conf_1" / "sum""""), ColumnName("conf_1"))
        )
      )
    )
    assert(expectedClassificationSQL.labelColumnSQL === sql.labelColumnSQL)
    assert(expectedClassificationSQL.confidenceSQL === sql.confidenceSQL)
    assert(expectedClassificationSQL.intermediateLayers === sql.intermediateLayers)
    assert(expectedClassificationSQL === sql)
  }

}

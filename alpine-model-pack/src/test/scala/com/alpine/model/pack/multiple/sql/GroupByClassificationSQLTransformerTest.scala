package com.alpine.model.pack.multiple.sql

import com.alpine.common.serialization.json.TypeWrapper
import com.alpine.json.ModelJsonUtil
import com.alpine.model.RowModel
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

  val modelA = MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
    "yes",
    Seq(0.5, -0.5).map(java.lang.Double.valueOf), 1.0)),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
  )

  val modelB = MultiLogisticRegressionModel(Seq(SingleLogisticRegression(
    "yes",
    Seq(0.1).map(java.lang.Double.valueOf), -10)),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long))
  )

  val groupModel = GroupByClassificationModel(ColumnDef("wind", ColumnType.String), Map("true" -> modelA, "false" -> modelB))

  val simpleSQLGenerator: SimpleSQLGenerator = new SimpleSQLGenerator()

  test("Should generate the correct SQL") {
    val sql = groupModel.sqlTransformer(simpleSQLGenerator).get.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sql, "\"demo\".\"golfnew\"", new AliasGenerator(), simpleSQLGenerator)
    val expectedSQL =
      """SELECT CASE WHEN "CONF0" IS NULL OR "CONF1" IS NULL
        |  THEN NULL
        |        ELSE (CASE WHEN ("CONF0" > "CONF1")
        |          THEN 'yes'
        |              ELSE 'no' END) END AS "PRED"
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

  test("Should generate the correct SQL for another model") {
    val model: RowModel = ModelJsonUtil.compactGsonBuilder.create().fromJson(GroupBySampleModels.model, classOf[TypeWrapper[RowModel]]).value
    val sql = model.sqlTransformer(simpleSQLGenerator).get.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sql, "\"demo\".\"golfnew\"", new AliasGenerator(), simpleSQLGenerator)
    // println(selectStatement)

    /**
      * There is redundancy in the SQL, which we should fix at some point.
      */
    val expectedSQL =
      """SELECT CASE WHEN "CONF0" IS NULL OR "CONF1" IS NULL
        |  THEN NULL
        |       ELSE (CASE WHEN ("CONF0" > "CONF1")
        |         THEN 'yes'
        |             ELSE 'no' END) END AS "PRED"
        |FROM (SELECT
        |        (CASE WHEN ("outlook" = 'overcast')
        |          THEN "CONF0"
        |         WHEN ("outlook" = 'rain')
        |           THEN "CONF0_1"
        |         WHEN ("outlook" = 'sunny')
        |           THEN "CONF0_2"
        |         ELSE NULL END) AS "CONF0",
        |        (CASE WHEN ("outlook" = 'overcast')
        |          THEN "CONF1"
        |         WHEN ("outlook" = 'rain')
        |           THEN "CONF1_1"
        |         WHEN ("outlook" = 'sunny')
        |           THEN "CONF1_2"
        |         ELSE NULL END) AS "CONF1"
        |      FROM (SELECT
        |              "ce0"       AS "CONF0",
        |              "baseVal"   AS "CONF1",
        |              "column_12" AS "CONF0_1",
        |              "column_13" AS "CONF1_1",
        |              "column_26" AS "CONF0_2",
        |              "column_27" AS "CONF1_2",
        |              "column_0"  AS "outlook"
        |            FROM (SELECT
        |                    1 / "sum"                 AS "baseVal",
        |                    "e0" / "sum"              AS "ce0",
        |                    1 / "column_10"           AS "column_13",
        |                    "column_11" / "column_10" AS "column_12",
        |                    1 / "column_24"           AS "column_27",
        |                    "column_25" / "column_24" AS "column_26",
        |                    "column_0"                AS "column_0"
        |                  FROM (SELECT
        |                          1 + "e0"        AS "sum",
        |                          "e0"            AS "e0",
        |                          1 + "column_9"  AS "column_10",
        |                          "column_9"      AS "column_11",
        |                          1 + "column_23" AS "column_24",
        |                          "column_23"     AS "column_25",
        |                          "column_0"      AS "column_0"
        |                        FROM (SELECT
        |                                EXP(11.566053522376023 + "temperature" * 1.1368683772161603E-13 +
        |                                    "humidity" * 1.4210854715202004E-13 + "wind_0" * 3.637978807091713E-12 +
        |                                    "wind_1" * 0.0)    AS "e0",
        |                                EXP(11.566053522373863 + "column_5" * -4.884981308350689E-15 +
        |                                    "column_7" * 5.828670879282072E-16 + "column_8" * -23.132107044747375 +
        |                                    "column_6" * 0.0)  AS "column_9",
        |                                EXP(-4.217191318784561 + "column_19" * -0.022443258429683213 +
        |                                    "column_21" * -0.052069985289528796 + "column_22" * 22.74983888277704 +
        |                                    "column_20" * 0.0) AS "column_23",
        |                                "column_0"             AS "column_0"
        |                              FROM (SELECT
        |                                      "column_0"      AS "temperature",
        |                                      "column_1"      AS "humidity",
        |                                      (CASE WHEN ("wind" = 'true')
        |                                        THEN 1
        |                                       WHEN "wind" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "wind_0",
        |                                      (CASE WHEN ("wind" = 'false')
        |                                        THEN 1
        |                                       WHEN "wind" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "wind_1",
        |                                      "column_3"      AS "column_5",
        |                                      "column_2"      AS "column_7",
        |                                      (CASE WHEN ("column_4" = 'true')
        |                                        THEN 1
        |                                       WHEN "column_4" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "column_8",
        |                                      (CASE WHEN ("column_4" = 'false')
        |                                        THEN 1
        |                                       WHEN "column_4" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "column_6",
        |                                      "column_17"     AS "column_19",
        |                                      "column_16"     AS "column_21",
        |                                      (CASE WHEN ("column_18" = 'true')
        |                                        THEN 1
        |                                       WHEN "column_18" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "column_22",
        |                                      (CASE WHEN ("column_18" = 'false')
        |                                        THEN 1
        |                                       WHEN "column_18" IS NOT NULL
        |                                         THEN 0
        |                                       ELSE NULL END) AS "column_20",
        |                                      "column_30"     AS "column_0"
        |                                    FROM (SELECT
        |                                            "temperature"   AS "column_0",
        |                                            "humidity"      AS "column_1",
        |                                            (CASE WHEN "wind" IN ('true', 'false')
        |                                              THEN "wind"
        |                                             ELSE NULL END) AS "wind",
        |                                            "temperature"   AS "column_3",
        |                                            "humidity"      AS "column_2",
        |                                            (CASE WHEN "wind" IN ('true', 'false')
        |                                              THEN "wind"
        |                                             ELSE NULL END) AS "column_4",
        |                                            "temperature"   AS "column_17",
        |                                            "humidity"      AS "column_16",
        |                                            (CASE WHEN "wind" IN ('true', 'false')
        |                                              THEN "wind"
        |                                             ELSE NULL END) AS "column_18",
        |                                            "outlook"       AS "column_30"
        |                                          FROM
        |                                            "demo"."golfnew") AS alias_0) AS alias_1) AS alias_2) AS alias_3) AS alias_4) AS alias_5) AS alias_6"""
        .stripMargin.replaceAll("\\s+", " ").replaceAllLiterally("\n", "")

    assert(expectedSQL === selectStatement)
  }

}

object GroupBySampleModels {

  val model =
    s"""{
  "type": "com.alpine.model.pack.multiple.GroupByClassificationModel",
  "data": {
    "groupByFeature": {
      "columnName": "outlook",
      "columnType": "String"
    },
    "modelsByGroup": {
      "overcast": {
        "type": "com.alpine.model.pack.multiple.PipelineClassificationModel",
        "data": {
          "preProcessors": [
            {
              "type": "com.alpine.model.pack.multiple.CombinerModel",
              "data": {
                "models": [
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.UnitModel",
                      "data": {
                        "inputFeatures": [
                          {
                            "columnName": "temperature",
                            "columnType": "Double"
                          },
                          {
                            "columnName": "humidity",
                            "columnType": "Double"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  },
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.preprocess.OneHotEncodingModel",
                      "data": {
                        "oneHotEncodedFeatures": [
                          {
                            "hotValues": [
                              "true",
                              "false"
                            ],
                            "baseValue": {
                              "type": "None"
                            }
                          }
                        ],
                        "inputFeatures": [
                          {
                            "columnName": "wind",
                            "columnType": "String"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  }
                ],
                "identifier": "",
                "inputFeatures": [
                  {
                    "columnName": "temperature",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "humidity",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "wind",
                    "columnType": "String"
                  }
                ]
              }
            }
          ],
          "finalModel": {
            "type": "com.alpine.model.pack.ml.MultiLogisticRegressionModel",
            "data": {
              "singleLORs": [
                {
                  "dependentValue": "yes",
                  "coefficients": [
                    1.1368683772161603E-13,
                    1.4210854715202004E-13,
                    3.637978807091713E-12,
                    0.0
                  ],
                  "bias": 11.566053522376023
                }
              ],
              "baseValue": "no",
              "dependentFeatureName": "play",
              "inputFeatures": [
                {
                  "columnName": "temperature",
                  "columnType": "Double"
                },
                {
                  "columnName": "humidity",
                  "columnType": "Double"
                },
                {
                  "columnName": "wind_0",
                  "columnType": "Int"
                },
                {
                  "columnName": "wind_1",
                  "columnType": "Int"
                }
              ],
              "identifier": "LOR"
            }
          },
          "identifier": ""
        }
      },
      "rain": {
        "type": "com.alpine.model.pack.multiple.PipelineClassificationModel",
        "data": {
          "preProcessors": [
            {
              "type": "com.alpine.model.pack.multiple.CombinerModel",
              "data": {
                "models": [
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.UnitModel",
                      "data": {
                        "inputFeatures": [
                          {
                            "columnName": "temperature",
                            "columnType": "Double"
                          },
                          {
                            "columnName": "humidity",
                            "columnType": "Double"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  },
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.preprocess.OneHotEncodingModel",
                      "data": {
                        "oneHotEncodedFeatures": [
                          {
                            "hotValues": [
                              "true",
                              "false"
                            ],
                            "baseValue": {
                              "type": "None"
                            }
                          }
                        ],
                        "inputFeatures": [
                          {
                            "columnName": "wind",
                            "columnType": "String"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  }
                ],
                "identifier": "",
                "inputFeatures": [
                  {
                    "columnName": "temperature",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "humidity",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "wind",
                    "columnType": "String"
                  }
                ]
              }
            }
          ],
          "finalModel": {
            "type": "com.alpine.model.pack.ml.MultiLogisticRegressionModel",
            "data": {
              "singleLORs": [
                {
                  "dependentValue": "yes",
                  "coefficients": [
                    -4.884981308350689E-15,
                    5.828670879282072E-16,
                    -23.132107044747375,
                    0.0
                  ],
                  "bias": 11.566053522373863
                }
              ],
              "baseValue": "no",
              "dependentFeatureName": "play",
              "inputFeatures": [
                {
                  "columnName": "temperature",
                  "columnType": "Double"
                },
                {
                  "columnName": "humidity",
                  "columnType": "Double"
                },
                {
                  "columnName": "wind_0",
                  "columnType": "Int"
                },
                {
                  "columnName": "wind_1",
                  "columnType": "Int"
                }
              ],
              "identifier": "LOR"
            }
          },
          "identifier": ""
        }
      },
      "sunny": {
        "type": "com.alpine.model.pack.multiple.PipelineClassificationModel",
        "data": {
          "preProcessors": [
            {
              "type": "com.alpine.model.pack.multiple.CombinerModel",
              "data": {
                "models": [
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.UnitModel",
                      "data": {
                        "inputFeatures": [
                          {
                            "columnName": "temperature",
                            "columnType": "Double"
                          },
                          {
                            "columnName": "humidity",
                            "columnType": "Double"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  },
                  {
                    "id": "",
                    "model": {
                      "type": "com.alpine.model.pack.preprocess.OneHotEncodingModel",
                      "data": {
                        "oneHotEncodedFeatures": [
                          {
                            "hotValues": [
                              "true",
                              "false"
                            ],
                            "baseValue": {
                              "type": "None"
                            }
                          }
                        ],
                        "inputFeatures": [
                          {
                            "columnName": "wind",
                            "columnType": "String"
                          }
                        ],
                        "identifier": ""
                      }
                    }
                  }
                ],
                "identifier": "",
                "inputFeatures": [
                  {
                    "columnName": "temperature",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "humidity",
                    "columnType": "Double"
                  },
                  {
                    "columnName": "wind",
                    "columnType": "String"
                  }
                ]
              }
            }
          ],
          "finalModel": {
            "type": "com.alpine.model.pack.ml.MultiLogisticRegressionModel",
            "data": {
              "singleLORs": [
                {
                  "dependentValue": "yes",
                  "coefficients": [
                    -0.022443258429683213,
                    -0.052069985289528796,
                    22.74983888277704,
                    0.0
                  ],
                  "bias": -4.217191318784561
                }
              ],
              "baseValue": "no",
              "dependentFeatureName": "play",
              "inputFeatures": [
                {
                  "columnName": "temperature",
                  "columnType": "Double"
                },
                {
                  "columnName": "humidity",
                  "columnType": "Double"
                },
                {
                  "columnName": "wind_0",
                  "columnType": "Int"
                },
                {
                  "columnName": "wind_1",
                  "columnType": "Int"
                }
              ],
              "identifier": "LOR"
            }
          },
          "identifier": ""
        }
      }
    },
    "identifier": "LOR"
  }
}"""

}

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

  test("Should generate the correct SQL for another model") {
    val model: RowModel = ModelJsonUtil.compactGsonBuilder.create().fromJson(GroupBySampleModels.model, classOf[TypeWrapper[RowModel]]).value
    val sql = model.sqlTransformer(simpleSQLGenerator).get.getSQL
    val selectStatement = SQLUtility.getSelectStatement(sql, "\"demo\".\"golfnew\"", new AliasGenerator(), simpleSQLGenerator)

    /**
      * There is redundancy in the SQL, which we should fix at some point.
      */
    val expectedSQL =
      """SELECT (CASE WHEN ("CONF0" > "CONF1")
        |  THEN 'yes'
        |        ELSE 'no' END) AS "PRED"
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
        |              "column_8"  AS "CONF0_1",
        |              "column_9"  AS "CONF1_1",
        |              "column_19" AS "CONF0_2",
        |              "column_20" AS "CONF1_2",
        |              "column_0"  AS "outlook"
        |            FROM (SELECT
        |                    1 / "sum"                 AS "baseVal",
        |                    "e0" / "sum"              AS "ce0",
        |                    1 / "column_6"            AS "column_9",
        |                    "column_7" / "column_6"   AS "column_8",
        |                    1 / "column_17"           AS "column_20",
        |                    "column_18" / "column_17" AS "column_19",
        |                    "column_0"                AS "column_0"
        |                  FROM (SELECT
        |                          1 + "e0"        AS "sum",
        |                          "e0"            AS "e0",
        |                          1 + "column_5"  AS "column_6",
        |                          "column_5"      AS "column_7",
        |                          1 + "column_16" AS "column_17",
        |                          "column_16"     AS "column_18",
        |                          "column_0"      AS "column_0"
        |                        FROM (SELECT
        |                                EXP(11.566053522376023 + "temperature" * 1.1368683772161603E-13 +
        |                                    "humidity" * 1.4210854715202004E-13 + "wind_0" * 3.637978807091713E-12 +
        |                                    "wind_1" * 0.0)    AS "e0",
        |                                EXP(11.566053522373863 + "column_1" * -4.884981308350689E-15 +
        |                                    "column_3" * 5.828670879282072E-16 + "column_4" * -23.132107044747375 +
        |                                    "column_2" * 0.0)  AS "column_5",
        |                                EXP(-4.217191318784561 + "column_12" * -0.022443258429683213 +
        |                                    "column_14" * -0.052069985289528796 + "column_15" * 22.74983888277704 +
        |                                    "column_13" * 0.0) AS "column_16",
        |                                "column_0"             AS "column_0"
        |                              FROM (SELECT
        |                                      "temperature" AS "temperature",
        |                                      "humidity"    AS "humidity",
        |                                      (CASE WHEN ("wind" = 'true')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "wind_0",
        |                                      (CASE WHEN ("wind" = 'false')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "wind_1",
        |                                      "temperature" AS "column_1",
        |                                      "humidity"    AS "column_3",
        |                                      (CASE WHEN ("wind" = 'true')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "column_4",
        |                                      (CASE WHEN ("wind" = 'false')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "column_2",
        |                                      "temperature" AS "column_12",
        |                                      "humidity"    AS "column_14",
        |                                      (CASE WHEN ("wind" = 'true')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "column_15",
        |                                      (CASE WHEN ("wind" = 'false')
        |                                        THEN 1
        |                                       ELSE 0 END)  AS "column_13",
        |                                      "outlook"     AS "column_0"
        |                                    FROM
        |                                      "demo"."golfnew") AS alias_0) AS alias_1) AS alias_2) AS alias_3) AS alias_4) AS alias_5"""
        .stripMargin.replaceAll("\\s+", " ").replaceAllLiterally("\n", "")

    assert(expectedSQL === selectStatement)
  }

}

object GroupBySampleModels {

  val model = s"""{
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

package com.alpine.model.pack.multiple

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.ml.{MultiLogisticRegressionModel, SingleLogisticRegression}
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/17/16.
  */
class GroupByClassificationModelTest extends FunSuite {

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

  test("Serialization should work.") {
    JsonTestUtil.testJsonization(groupModel)
  }

  test("testTransformer") {
    val groupByTransformer = groupModel.transformer
    val t1 = modelA.transformer
    val t2 = modelB.transformer
    Range(0, 20).foreach(i => {
      val row = List(math.random, math.random, math.random, math.random, math.random)
      val p1 = groupByTransformer.score("true" :: row)
      val q1 = t1.score(row)
      assert(p1 equals (q1, 1E-3))
      val p2 = groupByTransformer.score("false" :: row)
      val q2 = t2.score(Seq(row(1)))
      assert(p2 equals (q2, 1E-3))
    })
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[GroupByClassificationModel]).getSerialVersionUID === -7994790669603482144L)
  }
}

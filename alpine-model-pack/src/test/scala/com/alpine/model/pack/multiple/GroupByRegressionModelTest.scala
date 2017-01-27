package com.alpine.model.pack.multiple

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class GroupByRegressionModelTest extends FunSuite {

  val lir1 = LinearRegressionModel.make(
    Seq(1.0, 2.0, -1.0),
    Seq(
      ColumnDef("d", ColumnType.Double),
      ColumnDef("a", ColumnType.Double),
      ColumnDef("e", ColumnType.Double)
    ),
    0,
    "money"
  )

  val lir2 = LinearRegressionModel.make(
    Seq(-1.0, -2.0, 1.0),
    Seq(
      ColumnDef("b", ColumnType.Double),
      ColumnDef("c", ColumnType.Double),
      ColumnDef("e", ColumnType.Double)
    ),
    0,
    "money"
  )

  val groupByModel = new GroupByRegressionModel(ColumnDef("z", ColumnType.String), Map("zee" -> lir1, "zed" -> lir2))

  test("Serialization should work.") {
    JsonTestUtil.testJsonization(groupByModel)
  }

  test("testTransformer") {
    val groupByTransformer = groupByModel.transformer
    val t1 = lir1.transformer
    val t2 = lir2.transformer
    Range(0, 20).foreach(i => {
      val row = List(math.random, math.random, math.random, math.random, math.random)
      val p1 = groupByTransformer.predict("zee" :: row)
      val q1 = t1.predict(Seq(row(0), row(1), row(2)))
      assert(p1 == q1)
      val p2 = groupByTransformer.predict("zed" :: row)
      val q2 = t2.predict(Seq(row(3), row(4), row(2)))
      assert(p2 == q2)
    })
  }

  test("testInputFeatures") {
    assert(Seq(
      ColumnDef("z", ColumnType.String),
      ColumnDef("d", ColumnType.Double),
      ColumnDef("a", ColumnType.Double),
      ColumnDef("e", ColumnType.Double),
      ColumnDef("b", ColumnType.Double),
      ColumnDef("c", ColumnType.Double)
    ) ===
    groupByModel.inputFeatures)

  }

  test("testDependentFeature") {
    assert(
      ColumnDef("money", ColumnType.Double)
     ===
      groupByModel.dependentFeature
    )
  }

  val integerModel = new GroupByRegressionModel(ColumnDef("z", ColumnType.Long), Map(0 -> lir1, 1 -> lir2))

  test("testTransformer for integer group by") {
    val groupByTransformer = integerModel.transformer
    val t1 = lir1.transformer
    val t2 = lir2.transformer
    Range(0, 20).foreach(i => {
      val row = List(math.random, math.random, math.random, math.random, math.random)
      val p1 = groupByTransformer.predict(0 :: row)
      val q1 = t1.predict(Seq(row(0), row(1), row(2)))
      assert(p1 == q1)
      val p2 = groupByTransformer.predict(1 :: row)
      val q2 = t2.predict(Seq(row(3), row(4), row(2)))
      assert(p2 == q2)
    })
  }

  val doubleModel = new GroupByRegressionModel(ColumnDef("z", ColumnType.Double), Map(0.0 -> lir1, 1.5 -> lir2))

  test("testTransformer for double group by") {
    val groupByTransformer = doubleModel.transformer
    val t1 = lir1.transformer
    val t2 = lir2.transformer
    Range(0, 20).foreach(i => {
      val row = List(math.random, math.random, math.random, math.random, math.random)
      val p1 = groupByTransformer.predict(0.0 :: row)
      val q1 = t1.predict(Seq(row(0), row(1), row(2)))
      assert(p1 == q1)
      val p2 = groupByTransformer.predict(1.5 :: row)
      val q2 = t2.predict(Seq(row(3), row(4), row(2)))
      assert(p2 == q2)
    })
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[GroupByRegressionModel]).getSerialVersionUID === -941388806378184495L)
  }


}

package com.alpine.model.pack.multiple

import com.alpine.model.pack.UnitModel
import com.alpine.model.pack.ml.LinearRegressionModel
import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class PipelineTransformerTest extends FunSuite {

  test("Should handle reordering correctly") {
    val firstModel = UnitModel(Seq(
      ColumnDef("a", ColumnType.String),
      ColumnDef("b", ColumnType.String),
      ColumnDef("c", ColumnType.String),
      ColumnDef("d", ColumnType.String),
      ColumnDef("e", ColumnType.String)
    ))
    val secondModel = UnitModel(Seq(
      ColumnDef("b", ColumnType.String),
      ColumnDef("d", ColumnType.String),
      ColumnDef("a", ColumnType.String),
      ColumnDef("e", ColumnType.String)
    ))
    val pipelineModel = PipelineRowModel(Seq(firstModel, secondModel))
    val transformer = pipelineModel.transformer
    val actual = transformer.apply(Seq("A", "B", "C", "D", "E"))
    val expected = Seq("B", "D", "A", "E")
    assert(expected === actual)
  }

  test("Should handle reordering correctly with a model") {
    val firstModel = UnitModel(Seq(
      ColumnDef("a", ColumnType.Double),
      ColumnDef("b", ColumnType.Double),
      ColumnDef("c", ColumnType.Double),
      ColumnDef("d", ColumnType.Double),
      ColumnDef("e", ColumnType.Double)
    ))
    val secondModel = UnitModel(Seq(
      ColumnDef("b", ColumnType.Double),
      ColumnDef("d", ColumnType.Double),
      ColumnDef("a", ColumnType.Double),
      ColumnDef("e", ColumnType.Double)
    ))

    val lir = LinearRegressionModel.make(
      Seq(1.0, 2.0, -1.0),
      Seq(
        ColumnDef("d", ColumnType.Double),
        ColumnDef("a", ColumnType.Double),
        ColumnDef("e", ColumnType.Double)
      )
    )
    val pipelineModel = PipelineRegressionModel(Seq(firstModel, secondModel), lir)
    val transformer = pipelineModel.transformer
    val actual = transformer.predict(Seq(1.0, -5, 200, 3, 5))
    val expected = 3 + 2.0 - 5
    assert(expected === actual)
  }

}

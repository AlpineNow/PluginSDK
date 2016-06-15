/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.RowModel
import com.alpine.model.export.pfa.utils.JsonConverter
import com.alpine.model.pack.multiple.CombinerModel
import com.alpine.transformer.Transformer
import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.datatype.{AvroNull, AvroRecord, AvroType, AvroUnion}
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.reader.jsonToAst
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by Jennifer Thompson on 6/1/16.
  */
trait AlpinePFAConversionTest extends FunSuite with Matchers  {

  def fullCorrectnessTest(testModel: RowModel, testRows: Seq[Seq[Any]], printPFA: Boolean = false) = {
    simpleCorrectnessTest(testModel, testRows, printPFA)
    nameSpacingCorrectnessTest(testModel, testRows, printPFA)
    nameCollisionsCorrectnessTest(testModel, testRows, printPFA)
  }

  def simpleCorrectnessTest(testModel: RowModel, testRows: Seq[Seq[Any]], printPFA: Boolean): Unit = {
    val jsonPFA = ConverterLookup.findConverter(testModel).toJsonPFA
    if (printPFA) {
      println("Simple PFA is: ")
      println(jsonPFA)
    }
    val engineConfig: EngineConfig = jsonToAst.apply(jsonPFA)
    testPFA(engineConfig, testModel, testRows)
  }

  def nameSpacingCorrectnessTest(testModel: RowModel, testRows: Seq[Seq[Any]], printPFA: Boolean): Unit = {
    val jsonPFA = SharedPipelineCombinerLogic.getPFAComponents(testModel, "1_2_3", "input").toJson
    if (printPFA) {
      println("PFA with name-spacing is: ")
      println(jsonPFA)
    }
    val engineConfig: EngineConfig = jsonToAst.apply(jsonPFA)
    testPFA(engineConfig, testModel, testRows)
  }

  def nameCollisionsCorrectnessTest(testModel: RowModel, testRows: Seq[Seq[Any]], printPFA: Boolean): Unit = {
    val multiModel = CombinerModel.make(Seq(testModel, testModel))
    val jsonPFA = ConverterLookup.findConverter(multiModel).toJsonPFA
    if (printPFA) {
      println("PFA of duplicated model is: ")
      println(jsonPFA)
    }
    val engineConfig: EngineConfig = jsonToAst.apply(jsonPFA)
    testPFA(engineConfig, multiModel, testRows)
  }

  def testPFA(config: EngineConfig, model: RowModel, testRows: Seq[Seq[Any]]): Unit = {
    val pfaEngine = PFAEngine.fromAst(config).head
    val alpineTransformer = model.transformer
    testRows.foreach(row => {
      testRow(pfaEngine, alpineTransformer, row)
    })
  }

  def testRow(pfaEngine: PFAEngine[AnyRef, AnyRef], alpineTransformer: Transformer, row: Seq[Any]): Unit = {
    val alpineResult: Seq[Any] = alpineTransformer.apply(row)
    val pfaRecord: AnyRef = prepareEngineInput(pfaEngine, row)
    val pfaResult = pfaEngine.action(pfaRecord)
    val castedResult: PFARecord = pfaResult.asInstanceOf[PFARecord]
    assertResultsEqual(castedResult, alpineResult)
  }

  def prepareEngineInput(pfaEngine: PFAEngine[AnyRef, AnyRef], row: Seq[Any]): AnyRef = {
    val inputAsMap = (pfaEngine.inputType.asInstanceOf[AvroRecord].fields zip row).map {
      case (field, null) => (field.name, null)
      case (field, rowValue) =>
        field.avroType match {
          case unionType: AvroUnion =>
            val notNullType: AvroType = unionType.types.filterNot(_.isInstanceOf[AvroNull]).head
            (field.name, Map(notNullType.name -> rowValue))
          case _ =>
            (field.name, rowValue)
        }
    }.toMap
    val json: String = JsonConverter.anyToJson(inputAsMap)
    val pfaRecord = pfaEngine.jsonInput(json)
    pfaRecord
  }

  def assertResultsEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    assert(alpineResult === pfaRecord.getAll())
  }

  def assertDoublesEqual(x: Double, y: Double, tolerance: Double = 1e-7): Unit = {
    x should be (y +- 1e-7)
  }

  def assertRecordOfDoublesEqual(pfaRecord: PFARecord, alpineResult: Seq[Any]): Unit = {
    (alpineResult zip pfaRecord.getAll()).foreach {
      case (a, b) => assertDoublesEqual(a.asInstanceOf[Double], b.asInstanceOf[Double])
    }
  }

}

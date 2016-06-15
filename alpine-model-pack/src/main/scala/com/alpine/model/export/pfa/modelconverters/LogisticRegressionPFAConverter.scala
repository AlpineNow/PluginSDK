/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes._
import com.alpine.model.export.pfa.expressions._
import com.alpine.model.export.pfa.utils.CommonFunctions
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.ml.MultiLogisticRegressionModel

/**
  * Created by Jennifer Thompson on 5/26/16.
  */
class LogisticRegressionPFAConverter(model: MultiLogisticRegressionModel) extends PFAConverter {

  override def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    if (model.classLabels.length > 2) {
      PFAComponentsForMultiLOR(inputName, nameSpaceID)
    } else {
      PFAComponentsForSingleLOR(inputName, nameSpaceID)
    }
  }

  def PFAComponentsForMultiLOR(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)

    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val modelCellName = appendNameSpaceID(nameSpaceID, "model")
    val dependentValuesCellName = appendNameSpaceID(nameSpaceID, "dependentValues")

    val cells = Map(
      modelCellName -> CellInit(
        new ArrayType(new RecordType(
          "LinearModel",
          Seq(new FieldType("coeff", AvroTypes.arrayDouble), new FieldType("const", AvroTypes.double))
        )),
        model.singleLORs.map(singleLOR => Map("coeff" -> singleLOR.coefficients, "const" -> singleLOR.bias))
      ),
      dependentValuesCellName -> CellInit(
        AvroTypes.arrayString,
        // Same as model.distinctValues, but want to make clear that the baseValue is last.
        model.singleLORs.map(l => l.dependentValue) ++ List(model.baseValue)
      )
    )

    val zipDoubleMapFcnName = appendNameSpaceID(nameSpaceID, "zipDoubleMap")

    val action = {

      val convertToVector = let(
        "vector",
        recordAsArray(inputName, model.inputFeatures.map(_.columnName), AvroTypes.double)
      )

      val rawConfs = {
        val singleLoRFunction: PFAFunction = new PFAFunction(
          params = Seq(Map("linearModel" -> new AvroTypeReference("LinearModel"))),
          ret = AvroTypes.double,
          `do` = FunctionExecute("m.exp", FunctionExecute("model.reg.linear", "vector", "linearModel"))
        )
        val confidences = FunctionExecute("a.append",
          FunctionExecute("a.map", CellAccess(modelCellName), singleLoRFunction),
            1.0 // Need to append 1 for the base value.
        )
        let("rawConfs", confidences)
      }

      val prediction = {
        val index = FunctionExecute("a.argmax", "rawConfs")
        AttributeAccess(CellAccess(dependentValuesCellName), index)
      }

      // Normalize
      val sum = let("sum", Map("a.sum" -> "rawConfs"))
      val normalizedConfs = {
        // Sum > 0 since rawConfs contains 1 and no negative numbers.
        val divide = new PFAFunction(Map("x" -> AvroTypes.double), AvroTypes.double, FunctionExecute("/", "x", "sum"))
        let("normalizedConfs", FunctionExecute("a.map", "rawConfs", divide))
      }

      val maxConf = FunctionExecute("a.max", "normalizedConfs")

      val outputValues = Seq(prediction, maxConf, FunctionExecute(UDFAccess(zipDoubleMapFcnName), CellAccess(dependentValuesCellName), "normalizedConfs"))

      val record = new NewPFAObject(
        (model.outputFeatures.map(_.columnName) zip outputValues).toMap,
        outputType
      )

      Seq(convertToVector, rawConfs, sum, normalizedConfs, record)
    }

    new PFAComponents(
      input = inputType,
      output = outputType,
      cells = cells,
      action = action,
      fcns = Map(zipDoubleMapFcnName -> CommonFunctions.zipDoubleMap)
    )
  }

  /**
    * Significantly simpler (and possible faster than the multi-LoR version, so we keep it even though it
    * should be functionally equivalent.
    */
  def PFAComponentsForSingleLOR(inputName: String, nameSpaceID: Option[String]): PFAComponents = {
    val singleLOR = model.singleLORs.head

    val inputType = AvroTypes.fromAlpineSchema("input", model.inputFeatures)

    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val modelCellName = appendNameSpaceID(nameSpaceID, "model")
    val positiveValueCellName = appendNameSpaceID(nameSpaceID, "positiveValue")
    val baseValueCellName = appendNameSpaceID(nameSpaceID, "baseValue")

    val cells = Map(
      modelCellName -> CellInit(
        new RecordType(
          "Model",
          Seq(new FieldType("coeff", AvroTypes.arrayDouble), new FieldType("const", AvroTypes.double))
        ),
        Map("coeff" -> singleLOR.coefficients, "const" -> singleLOR.bias)
      ),
      positiveValueCellName -> CellInit(
        AvroTypes.string,
        singleLOR.dependentValue
      ),
      baseValueCellName -> CellInit(
        AvroTypes.string,
        model.baseValue
      )
    )

    val action = {
      val convertToVector = let(
        "vector",
        recordAsArray(inputName, model.inputFeatures.map(_.columnName), AvroTypes.double)
      )
      val confidence = let("conf", FunctionExecute("m.link.logit", FunctionExecute("model.reg.linear", "vector", CellAccess(modelCellName))))
      val prediction = Map("if" -> FunctionExecute(">", "conf", 0.5), "then" -> CellAccess(positiveValueCellName), "else" -> CellAccess(baseValueCellName))
      val record = new NewPFAObject(
        Map(
          model.outputFeatures.head.columnName -> prediction,
          model.outputFeatures(1).columnName -> FunctionExecute("max", "conf", FunctionExecute("-", 1, "conf")),
          model.outputFeatures(2).columnName -> NewPFAObject(
            `new` = Map(
              model.baseValue -> FunctionExecute("-", 1, "conf"),
              singleLOR.dependentValue -> "conf"
            ),
            `type` = AvroTypes.mapDouble
          )
        ),
        outputType
      )
      Seq(convertToVector, confidence, record)
    }

    new PFAComponents(
      input = inputType,
      output = outputType,
      cells = cells,
      action = action
    )
  }

}

/*
Old version:
input: {type: array, items: double}
output:
  type: record
  name: Output
  fields:
    - {name: prediction, type: boolean}
    - {name: confidence, type: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [2.0,-3.0]
      const: 4.0
action:
  - let:
      conf:
        m.link.logit:
          model.reg.linear:
            - input
            - cell: model
  - let:
      prediction: {">": [conf, 0.5]}
  - "new": {prediction: prediction, confidence: conf}
    type: Output
 */

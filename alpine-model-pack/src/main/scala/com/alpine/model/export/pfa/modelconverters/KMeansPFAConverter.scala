/*
 * COPYRIGHT (C) Jun 10 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.model.export.pfa.modelconverters

import com.alpine.model.export.pfa.{PFAConverter, PFAComponents}
import com.alpine.model.export.pfa.avrotypes.{ArrayType, AvroTypes}
import com.alpine.model.export.pfa.expressions._
import com.alpine.model.export.pfa.utils.CommonFunctions
import com.alpine.model.export.pfa.utils.ExpressionUtil._
import com.alpine.model.pack.ml.KMeansModel

/**
  * Created by jenny on 5/26/16.
  */
class KMeansPFAConverter(model: KMeansModel) extends PFAConverter {

  def toPFAComponents(inputName: String, nameSpaceID: Option[String]): PFAComponents = {

    val modelCellName = appendNameSpaceID(nameSpaceID, "clusterModel")
    val clusterNamesCellName = appendNameSpaceID(nameSpaceID, "clusterNames")
    val zipDoubleMapFcnName = appendNameSpaceID(nameSpaceID, "zipDoubleMap")

    val cells = Map(
      modelCellName -> CellInit(
        ArrayType(AvroTypes.arrayDouble),
        model.clusters.map(_.centroid)
      ),
      clusterNamesCellName -> CellInit(
        AvroTypes.arrayString,
        model.clusters.map(_.name)
      )
    )

    val outputType = outputTypeFromAlpineSchema(nameSpaceID, model.outputFeatures)

    val action = {
      val convertToVector = let(
        "vector",
        recordAsArray(inputName, model.inputFeatures.map(_.columnName), AvroTypes.double)
      )

      val kMeansDistances = let("distances", FunctionExecute(
        functionName = "a.map",
        firstArg = CellAccess(modelCellName),
        secondArg = PFAFunction(
          Seq(Map("x" -> AvroTypes.arrayDouble)),
          AvroTypes.double,
          FunctionExecute("metric.euclidean", FcnRef("metric.absDiff"), "x", "vector")
        )
      ))

      val prediction = {
        val index = FunctionExecute("a.argmin", "distances")
        AttributeAccess(CellAccess(clusterNamesCellName), index)
      }

      val minDistance = FunctionExecute("a.min", "distances")

      val outputValues = Seq(prediction, minDistance, FunctionExecute(UDFAccess(zipDoubleMapFcnName), CellAccess(clusterNamesCellName), "distances"))

      val record = NewPFAObject(
        (model.outputFeatures.map(_.columnName) zip outputValues).toMap,
        outputType
      )
      Seq(convertToVector, kMeansDistances, record)
    }

    PFAComponents(
      input = AvroTypes.fromAlpineSchema("input", model.inputFeatures),
      output = outputType,
      cells = cells,
      action = action,
      fcns = Map(zipDoubleMapFcnName -> CommonFunctions.zipDoubleMap)
    )
  }

}

/*
Early version was:

input: {type: array, items: double}
output: string
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: {type: array, items: double}}
          - {name: id, type: string}
    init:
      - {id: one, center: [1, 1, 1, 1, 1]}
      - {id: two, center: [2, 2, 2, 2, 2]}
      - {id: three, center: [3, 3, 3, 3, 3]}
      - {id: four, center: [4, 4, 4, 4, 4]}
      - {id: five, center: [5, 5, 5, 5, 5]}
action:
  attr:
    model.cluster.closest:
      - input
      - cell: clusters
      - params:
          - x: {type: array, items: double}
          - y: {type: array, items: double}
        ret: double
        do:
          metric.euclidean:
            - fcn: metric.absDiff
            - x
            - y
  path: [[id]]
*/

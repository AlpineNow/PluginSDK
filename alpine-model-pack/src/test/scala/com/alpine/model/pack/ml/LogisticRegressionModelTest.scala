package com.alpine.model.pack.ml

import java.io.ObjectStreamClass

import com.alpine.json.JsonTestUtil
import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import com.alpine.result.ClassificationResult
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 2/9/16.
  */
class LogisticRegressionModelTest extends FunSuite {

  val lor = MultiLogisticRegressionModel(Seq(
    SingleLogisticRegression(
      "yes",
      Seq(2.0, 3.0).map(java.lang.Double.valueOf), 4.0
    )),
    "no",
    "play",
    Seq(ColumnDef("temperature", ColumnType.Long), ColumnDef("humidity", ColumnType.Long))
  )

  test("Should serialize correctly") {
    JsonTestUtil.testJsonization(lor)
  }

  test("Should do in-memory prediction correctly") {
    val transformer = lor.transformer

    val testRow: Seq[Double] = Seq(3.5, -4.5)
    val expectedYesConf = math.exp((testRow zip Seq(2.0, 3.0)).map(x => x._1 * x._2).sum + 4.0)
    val expectedNoConf = 1
    val normalizingConstant = expectedNoConf + expectedYesConf

    val expectedResult = ClassificationResult(
      Seq("yes", "no"),
      Array(expectedYesConf / normalizingConstant, expectedNoConf / normalizingConstant)
    )

    val actualResult = transformer.score(testRow)
    assert(expectedResult.toMap === actualResult.toMap)
    assert(expectedResult.equals(actualResult, 1E-6))
  }

  test("Serial UID should be stable") {
    assert(ObjectStreamClass.lookup(classOf[MultiLogisticRegressionModel]).getSerialVersionUID === 1381676143872694894L)
  }

}

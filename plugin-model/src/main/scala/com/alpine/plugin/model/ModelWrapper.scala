/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.model

import com.alpine.model._
import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.{IOBase, OperatorInfo}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Model APIs.
 */
@AlpineSdkApi
abstract class ModelWrapper[M <: MLModel](
  val modelName: String,
  val model: M,
  val sourceOperatorInfo: Option[OperatorInfo],
  val addendum: Map[String, AnyRef]) extends IOBase {
  override def displayName: String = modelName
}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Classification Model APIs.
 */
@AlpineSdkApi
class ClassificationModelWrapper(
  modelName: String,
  model: ClassificationRowModel,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[ClassificationRowModel](modelName, model, sourceOperatorInfo, addendum) {}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Clustering Model APIs.
 */
@AlpineSdkApi
class ClusteringModelWrapper(
  override val modelName: String,
  override val model: ClusteringRowModel,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[ClusteringRowModel](modelName, model, sourceOperatorInfo, addendum) {}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Regression Model APIs.
 */
@AlpineSdkApi
class RegressionModelWrapper(
  modelName: String,
  model: RegressionRowModel,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[RegressionRowModel](modelName, model, sourceOperatorInfo, addendum) {}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Transformer APIs.
 */
@AlpineSdkApi
class TransformerWrapper(
  modelName: String,
  model: RowModel,
  override val sourceOperatorInfo: Option[OperatorInfo],
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[RowModel](modelName, model, sourceOperatorInfo, addendum) {}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.model

import com.alpine.model._
import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.IOBase

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Model APIs.
 */
@AlpineSdkApi
abstract class ModelWrapper[M <: MLModel](
  val model: M,
  val addendum: Map[String, AnyRef]) extends IOBase {
}

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Classification Model APIs.
 */
@AlpineSdkApi
class ClassificationModelWrapper(
  model: ClassificationRowModel,
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[ClassificationRowModel](model, addendum)
/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Clustering Model APIs.
 */
@AlpineSdkApi
class ClusteringModelWrapper(
  override val model: ClusteringRowModel,
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[ClusteringRowModel](model, addendum)

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Regression Model APIs.
 */
@AlpineSdkApi
class RegressionModelWrapper(
  model: RegressionRowModel,
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[RegressionRowModel](model, addendum)

/**
 * :: AlpineSdkApi ::
 * A wrapper around objects that implement the Alpine Transformer APIs.
 */
@AlpineSdkApi
class TransformerWrapper(
  model: RowModel,
  override val addendum: Map[String, AnyRef] = Map[String, AnyRef]()
) extends ModelWrapper[RowModel](model, addendum)

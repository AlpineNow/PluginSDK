/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.spark

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.{OperatorListener, OperatorParameters}


/**
  * :: AlpineSdkApi ::
  *
  * This is the Spark Job class.
  * It gets submitted to the cluster by a Runtime class which is a derivative of
  * SparkRuntime. The 'onExecution method' in this class serves as the driver function
  * for the Spark Job.
  *
  * It handles the serialization/deserialization of
  * Inputs and Outputs. It enables you to directly work with IOBase objects without needing to
  * implement your own (de)serialization logic. This class is intended to be coupled with
  * SparkRuntimeWithIOTypedJob, a descendant of SparkRuntime that takes a descendant of this class
  * as a generic parameter.
  *
  * Note: It is possible to use this class with a runtime class that extends the generic
  * SparkRuntime class (rather than the SparkRuntimeWithIOTypedJob class). However, by using
  * SparkRuntimeWithIOTypedJob and overriding the onExecution method, you can get many of the
  * benefits of the class while implementing more complex behavior. In taking the later approach
  * you can use the SparkRuntimeWithIOTypedJob implementation of the 'onExecution'
  * method as a utility function for submitting the Spark job by calling super.onExecution.
  *
  * @tparam I input type of your plugin must be consistent with the SparkRuntime implementation's
  *           type parameters.
  * @tparam O output type of your plugin
  */
@AlpineSdkApi
abstract class SparkIOTypedPluginJob[I, O] {

  /**
    * The driver function for the Spark job.
    * Unlike the corresponding function in the parent class, this function allows you to work with
    * IOBase types directly. YOU MUST Override one of the two 'onExecution' methods.
    *
    * @param alpineSparkEnvironment   Information about the spark job including the
    *                                 Spark session (unified spark context) created when job was submitted

    * @param input              the ioBase object which you have defined as the input to your plugin.
    *                           For example, if the GUI node of the plugin takes an HDFSTabularDataset,
    *                           this input parameter will be that dataset.
    * @param operatorParameters -the parameter values set in the GUI node. Their value can be
    *                           accessed via the "key" defined for each parameter added to the
    *                           OperatorDialog in the GUI node.
    * @param listener           a listener object which allows you to send messages to the Alpine GUI during
    *                           the Spark job
    * @return the output of your plugin
    */
  @throws[Exception]
  def onExecution(
    alpineSparkEnvironment: AlpineSparkEnvironment,
    input: I,
    operatorParameters: OperatorParameters,
    listener: OperatorListener): O
}

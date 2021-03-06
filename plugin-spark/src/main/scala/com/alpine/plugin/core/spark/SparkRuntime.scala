/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.spark

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io._
import com.alpine.plugin.core.{OperatorRuntime, _}
import com.alpine.plugin.generics.GenericUtils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * :: AlpineSdkApi ::
  */
/**
  * Defines the behavior of your plugin after the end user 'runs' it from the GUI.
  * This is a direct descendant of OperatorRuntime and it takes SparkExecutionContext as an argument.
  * Its 'onExecution' runs on the local machine (Alpine machine). It is in this 'onExecution'
  * method (call with'super.onExecution') that you could submit a Spark job.
  * Unlike its less generic descendant, SparkRuntimeWithIOTypedJob, which automatically submits a
  * SparkJob, you can use this class for more elaborate runtime behavior.
  * For example, you can do some local processing here before submitting a SparkPluginJob
  * to the cluster manually. Or you could can also do some purely local computations and just
  * return without submitting a job.
  *
  * @tparam I the IOBase input type of your plugin (must be consistent with the input type of the
  *           GUINode class implementation, the plugin signature implementation.
  * @tparam O the output of your plugin.
  */
@AlpineSdkApi
abstract class SparkRuntime[I <: IOBase, O <: IOBase] extends
  OperatorRuntime[SparkExecutionContext, I, O] {
}

/**
  * A descendant of SparkRuntime which handles the most straightforward runtime behavior,
  * submitting a Spark job with the input of your plugin and returning the output type.
  * It takes an implementation of SparkIOTypedPluginJob as a generic parameter, and it is in that
  * class where you will define the logic of the Spark job. Use this class if all you want to do is
  * submit a Spark job since it takes care of submitting the Spark job and serializing/
  * de-serializing the outputs.
  *
  * @tparam J your implementation of the SparkIOTypedPluginJob class, whose type parameters
  *           must align with I an O here
  * @tparam I the IOBase input type of your plugin (must be consistent with the input type of the
  *           GUINode class implementation, and the plugin signature implementation.
  * @tparam O the output of your plugin.
  *
  *           Note: In order to do more at runtime than just submit a Spark job, but use our
  *           serialization logic, you could use this class with SparkIOTypedPluginJob and simply override the
  *           'onExecution' method (see SparkIOTypedPluginJob documentation for more).
  */

abstract class SparkRuntimeWithIOTypedJob[
J <: SparkIOTypedPluginJob[I, O],
I <: IOBase,
O <: IOBase] extends OperatorRuntime[SparkExecutionContext, I, O] {
  private var submittedJob: SubmittedSparkJob[O] = null

  /**
    * The runtime behavior of the plugin. This method is called when the user clicks 'run' or
    * 'step run in the GUI'. The default implementation
    * --configures the Spark job as defined by the getSparkJobConfiguration
    * --submits a Spark job with the input dataType the parameters, the application context,
    * and the listener
    * --de-serializes the output returned by the Spark job
    * --returns the de-serialized output of the Spark job as an IOBase output object.
    *
    * @param context  A Spark specific execution context, includes Spark parameters.
    * @param input    The input to the operator.
    * @param params   The parameter values to the operator.
    * @param listener The listener object to communicate information back to
    *                 the console or the Alpine UI.
    * @return The output from the execution.
    */
  @throws[Exception]
  def onExecution(
                   context: SparkExecutionContext,
                   input: I,
                   params: OperatorParameters,
                   listener: OperatorListener): O = {
    val genericTypeArgs =
      GenericUtils.getAncestorClassGenericTypeArguments(
        this.getClass,
        "com.alpine.plugin.core.spark.SparkRuntimeWithIOTypedJob"
      )

    val sparkJobConfiguration: SparkJobConfiguration = getSparkJobConfiguration(params, input)

    val jobClass = genericTypeArgs.get("J").asInstanceOf[Class[J]]
    val jobTypeName = jobClass.getName
    listener.notifyMessage("Submitting " + jobTypeName)
    submittedJob = context.submitJob(
      jobClass,
      input,
      params,
      sparkJobConfiguration,
      listener
    )

    Await.result(submittedJob.future, Duration.Inf)
  }

  /**
    * The default implementation looks for the parameter values that would be included
    * by [[com.alpine.plugin.core.utils.SparkParameterUtils.addStandardSparkOptions]]. If these are
    * not provided we call out to Alpine's Spark Auto Tuning algorithm, which will determine them.
    * The result of this method is an object which we will use to determine the Spark settings.
    * The SparkJobConfiguration object contains three fields.
    * 1. A map with the advanced parameters. This should be the parameters in the "Advanced Spark
    * Parameters" box. However, if you would like to modify these values, or add your own Spark options
    * here, you may do that by adding those values to this object.
    * 2. A boolean "autoTuneMissingValues". If set to false, this will disable the auto tuning. In this
    * case you must fill in the values of
    * "spark.executor.memory" "spark.driver.memory" and "spark.executor.instances"
    * in the "userDefinedParameters" object
    * 3. Options that will be used for the Auto Tuning.
    * See com.alpine.plugin.core.spark.SparkJobConfiguration for details
    *
    * @param parameters Parameters of the operator.
    * @param input      The input to the operator.
    * @return The Spark job configuration that will be used to submit the Spark job.
    */
  def getSparkJobConfiguration(parameters: OperatorParameters, input: I): SparkJobConfiguration = {
    val params = parameters.getAdvancedSparkParameters
    SparkJobConfiguration(userDefinedParameters = params, autoTuneMissingValues = true,
      autoTunerOptions = getAutoTuningOptions(parameters, input))
  }


  /**
    * Set the options passed to our Spark Auto Tuner which will choose optimal Spark configuration
    * settings for values not provided by the user based on the size of the cluster, the input
    * data and the type of computation.
    * See documentation for the AutoTunerOptions object for more details on what the settings in this
    * object mean.
    * Set only the auto tuning options by overriding this method.
    * To change the parameters passed the Spark Configuration more comprehensively override
    * 'getSparkJobConfiguration' and this method will be ignored.
    */
  def getAutoTuningOptions(parameters: OperatorParameters, input: I): AutoTunerOptions = {
    val inputCachedSizeMultiplier = 1.0
    AutoTunerOptions(1.0, inputCachedSizeMultiplier, minExecutorMemory = 0L)
  }


  def onStop(
              context: SparkExecutionContext,
              listener: OperatorListener): Unit = {
    if (submittedJob != null) {
      submittedJob.cancel()

      listener.notifyMessage("Terminated the running job " + submittedJob.getJobName)
    } else {
      listener.notifyMessage("No running Spark job was found.")
    }
  }
}
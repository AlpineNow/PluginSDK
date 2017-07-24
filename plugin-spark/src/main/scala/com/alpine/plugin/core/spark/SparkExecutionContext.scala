/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.spark

import java.io.{InputStream, OutputStream}

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.io.IOBase
import com.alpine.plugin.core.visualization.HDFSVisualModelHelper
import com.alpine.plugin.core.{ExecutionContext, OperatorListener, OperatorParameters}
import org.apache.hadoop.fs.FileSystem

import scala.collection.immutable
import scala.concurrent.Future

/**
  * :: AlpineSdkApi ::
  *
  * @tparam O Output type of the submitted Spark job.
  */
trait SubmittedSparkJob[O] {
  def getJobName: String

  def future: Future[O]

  def cancel(): Unit
}

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
trait SparkExecutionContext extends ExecutionContext {
  /**
    * This is to be the function to submit the IO typed job to Spark.
    * IO typed Spark jobs will automatically serialize/deserialize input/outputs.
    * TODO: Not supported as of yet.
    *
    * @param jobClass  IO typed job class.
    * @param input     Input to the job. This automatically gets serialized.
    * @param params    Parameters into the job.
    * @param sparkConf Spark job configuration.
    * @param listener  Listener to pass to the job. The spark job should be
    *                  able to communicate directly with Alpine as it's running.
    * @tparam I   Input type.
    * @tparam O   Output type.
    * @tparam JOB The job type.
    * @return A submitted job object.
    */
  def submitJob[I <: IOBase, O <: IOBase, JOB <: SparkIOTypedPluginJob[I, O]](
    jobClass: Class[JOB],
    input: I,
    params: OperatorParameters,
    sparkConf: SparkJobConfiguration,
    listener: OperatorListener
  ): SubmittedSparkJob[O]

  def doHdfsAction[T](fs: FileSystem => T): T

  /**
    * Open a HDFS path for reading.
    *
    * @param path The HDFS path that we want to read from.
    * @return InputStream corresponding to the path.
    */
  def openPath(path: String): InputStream

  /**
    * Create a HDFS path for writing.
    *
    * @param path      The HDFS path that we want to create and write to.
    * @param overwrite Whether to overwrite the given path if it exists.
    * @return OutputStream corresponding to the path.
    */
  def createPath(path: String, overwrite: Boolean): OutputStream

  /**
    * Append contents to the given HDFS path.
    *
    * @param path The HDFS path that we want to append to.
    * @return OutputStream corresponding to the path.
    */
  def appendPath(path: String): OutputStream

  /**
    * Delete the given HDFS path.
    *
    * @param path      The HDFS path that we want to delete.
    * @param recursive If it's a directory, whether we want to delete
    *                  the directory recursively.
    * @return true if successful, false otherwise.
    */
  def deletePath(path: String, recursive: Boolean): Boolean

  /**
    * Determine whether the given path exists in the HDFS or not.
    *
    * @param path The path that we want to check.
    * @return true if it exists, false otherwise.
    */
  def exists(path: String): Boolean

  /**
    * Create the directory path.
    *
    * @param path The directory path that we want to create.
    * @return true if it succeeds, false otherwise.
    */
  def mkdir(path: String): Boolean

  def visualModelHelper: HDFSVisualModelHelper

  /**
    * Returns the map of Spark parameters after autoTuning algorithm is applied.
    * This is the final set of Spark properties ready to be passed with Spark job submission (except spark.job.name)
    * It leverages:
    * - user-defined Spark properties at the operator level (Spark Advanced Settings box), workflow level
    * and data source level (in this order of precedence).
    * - AutoTunerOptions set in the SparkJobConfiguration (@param sparkConf)
    * - auto-tuned parameters that were not user-specified
    *
    * @param jobClass  IO typed job class.
    * @param input     Input to the job. This automatically gets serialized.
    * @param params    Parameters into the job.
    * @param sparkConf Spark job configuration.
    * @param listener  Listener to pass to the job. The spark job should be
    *                  able to communicate directly with Alpine as it's running.
    * @tparam I Input type.
    * @return The map of relevant Spark properties after auto tuning algorithm was applied.
    */
  def getSparkAutoTunedParameters[I <: IOBase, JOB <: SparkIOTypedPluginJob[I, _]](
    jobClass: Class[JOB],
    input: I,
    params: OperatorParameters,
    sparkConf: SparkJobConfiguration,
    listener: OperatorListener
  ): immutable.Map[String, String]
}

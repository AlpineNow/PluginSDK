/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.spark

import java.io.{OutputStream, InputStream}

import scala.concurrent.Future

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.{OperatorListener, OperatorParameters, ExecutionContext}
import com.alpine.plugin.core.io.IOBase

/**
 * :: AlpineSdkApi ::
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
   * @param jobClass IO typed job class.
   * @param input Input to the job. This automatically gets serialized.
   * @param params Parameters into the job.
   * @param sparkConf Spark job configuration.
   * @param listener Listener to pass to the job. The spark job should be
   *                 able to communicate directly with Alpine as it's running.
   * @tparam I Input type.
   * @tparam O Output type.
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

  /**
   * Open a HDFS path for reading.
   * @param path The HDFS path that we want to read from.
   * @return InputStream corresponding to the path.
   */
  def openPath(path: String): InputStream

  /**
   * Create a HDFS path for writing.
   * @param path The HDFS path that we want to create and write to.
   * @param overwrite Whether to overwrite the given path if it exists.
   * @return OutputStream corresponding to the path.
   */
  def createPath(path: String, overwrite: Boolean): OutputStream

  /**
   * Append contents to the given HDFS path.
   * @param path The HDFS path that we want to append to.
   * @return OutputStream corresponding to the path.
   */
  def appendPath(path: String): OutputStream

  /**
   * Delete the given HDFS path.
   * @param path The HDFS path that we want to delete.
   * @param recursive If it's a directory, whether we want to delete
   *                  the directory recursively.
   * @return true if successful, false otherwise.
   */
  def deletePath(path: String, recursive: Boolean): Boolean

  /**
   * Determine whether the given path exists in the HDFS or not.
   * @param path The path that we want to check.
   * @return true if it exists, false otherwise.
   */
  def exists(path: String): Boolean

  /**
   * Create the directory path.
   * @param path The directory path that we want to create.
   * @return true if it succeeds, false otherwise.
   */
  def mkdir(path: String): Boolean
}

/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.test.mock

import java.io.{File, InputStream, OutputStream}

import com.alpine.plugin.core.config.CustomOperatorConfig
import com.alpine.plugin.core.{ChorusUserInfo, OperatorListener, OperatorParameters, WorkflowInfo}
import com.alpine.plugin.core.io.IOBase
import com.alpine.plugin.core.spark.{SparkExecutionContext, SparkIOTypedPluginJob, SparkJobConfiguration, SubmittedSparkJob}
import com.alpine.plugin.core.utils.ChorusAPICaller
import com.alpine.plugin.core.visualization.HDFSVisualModelHelper
import org.apache.hadoop.fs.FileSystem

/**
  * This is a mock version of SparkExecutionContext, for use in tests.
  * This defines the HDFSVisualModelHelper as HDFSVisualModelHelperMock,
  * and chorusAPICaller according to the argument.
  *
  * It can be extended for different behaviour (e.g. mocking the file system).
  */
class SparkExecutionContextMock(chorusAPICallerMock: ChorusAPICaller) extends SparkExecutionContext {

  override def submitJob[I <: IOBase, O <: IOBase, JOB <: SparkIOTypedPluginJob[I, O]](jobClass: Class[JOB], input: I, params: OperatorParameters, sparkConf: SparkJobConfiguration, listener: OperatorListener): SubmittedSparkJob[O] = ???

  override def doHdfsAction[T](fs: (FileSystem) => T): T = ???

  override def openPath(path: String): InputStream = ???

  override def createPath(path: String, overwrite: Boolean): OutputStream = ???

  override def appendPath(path: String): OutputStream = ???

  override def deletePath(path: String, recursive: Boolean): Boolean = ???

  override def exists(path: String): Boolean = ???

  override def mkdir(path: String): Boolean = ???

  override def visualModelHelper: HDFSVisualModelHelper = new HDFSVisualModelHelperMock

  override def chorusUserInfo: ChorusUserInfo = ???

  override def chorusAPICaller: ChorusAPICaller = chorusAPICallerMock

  override def workflowInfo: WorkflowInfo = ???

  override def recommendedTempDir: File = ???

  override def config: CustomOperatorConfig = ???
}

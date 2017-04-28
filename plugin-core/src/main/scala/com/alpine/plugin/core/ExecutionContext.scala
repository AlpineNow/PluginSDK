/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core

import java.io.File

import com.alpine.plugin.core.config.CustomOperatorConfig
import com.alpine.plugin.core.utils.ChorusAPICaller

/**
  * The context for a plugin execution. This contains information about the
  * underlying platform, such as connection information and/or job submission,
  * the working directory, etc.
  */
trait ExecutionContext {
  def chorusUserInfo: ChorusUserInfo

  def chorusAPICaller: ChorusAPICaller

  def workflowInfo: WorkflowInfo

  def recommendedTempDir: File

  def config: CustomOperatorConfig

}

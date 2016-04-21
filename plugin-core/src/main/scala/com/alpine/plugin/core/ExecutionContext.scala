/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

/**
 * The context for a plugin execution. This contains information about the
 * underlying platform, such as connection information and/or job submission,
 * the working directory, etc.
 */
trait ExecutionContext {
  def chorusUserInfo: ChorusUserInfo
}

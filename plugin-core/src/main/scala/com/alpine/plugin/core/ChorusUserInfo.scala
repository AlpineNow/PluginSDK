package com.alpine.plugin.core

import com.alpine.plugin.core.dialog.ChorusFile

/**
  * ChorusUserInfo
  *
  * @param userID    -- Chorus UserId (should be an integer).
  * @param userName  -- User Display Name, Chorus user name, not userId.
  * @param sessionID -- Chorus Session ID, can be used to make requests to Chorus.
  */
case class ChorusUserInfo(userID: String, userName: String, sessionID: String)

/**
  * @param workflowID -- ID of the workfile where the operator is run. This can be usd in the
  *                   ChorusAPICaller to get the workspace ID.
  */
case class WorkflowInfo(workflowID: String)


case class NotebookDetails(readyToExecuteDB: Boolean,
  readyToExecuteHD: Boolean,
  useSpark: Boolean,
  inputsMetadata: Map[String, NotebookIOInfo],
  outputsMetadata: Map[String, NotebookIOInfo])


case class PythonNotebook(chorusFile: ChorusFile,
                          userModifiedAt: String,
                          serverStatus: String)


case class PythonNotebookExecution(notebookId: String,
                                   executionId: String,
                                   status: String,
                                   response: Option[String],
                                   error: Option[String],
                                   isFinished: Boolean)


case class NotebookIOInfo(dataSourceName: String,
  isHdfsInput: Boolean,
  delimiter: String,
  quote: String,
  escape: String,
  columns: Seq[(String, String)],
  usesSpark: Boolean,
  inputName: Option[String])

case class ChorusDBSourceInfo(name: String, id: Int)

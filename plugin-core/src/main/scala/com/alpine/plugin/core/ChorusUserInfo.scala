package com.alpine.plugin.core

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
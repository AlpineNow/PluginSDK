/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.db

import java.sql.Connection

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.plugin.core.ExecutionContext

/**
 * :: AlpineSdkApi ::
 * DB connection and related info.
 * @param connection JDBC connection object. This should not be
 *                   manually closed by the plugin.
 * @param name Name representing the database within Alpine/Chorus.
 * @param url The database URL.
 * @param userName User name to the database.
 * @param password Password to the database.
 */
@AlpineSdkApi
case class DBConnectionInfo(
  connection: Connection,
  name: String,
  url: String,
  userName: String,
  password: String)

/**
 * :: AlpineSdkApi ::
 * For Database operators. This will contain database connection information (
 * e.g. JDBC interface), etc.
 */
@AlpineSdkApi
trait DBExecutionContext extends ExecutionContext {
  def getDBConnectionInfo: DBConnectionInfo
}

/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.io

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * :: AlpineSdkApi ::
 * Used to represent a Hive table.
 * Contains the Hive database name (optional), table name,
 * and tabular schema (column descriptions).
 */
@AlpineSdkApi
trait HiveTable extends IOBase with TabularDataset {
  def tabularSchema: TabularSchema
  def tableName: String
  def dbName: Option[String]
  def getConcatenatedName: String
}

/**
 * :: AlpineSdkApi ::
 * Contains helper methods for HiveTable.
 */
@AlpineSdkApi
object HiveTable {
  /**
   * Returns a string with the full location of the Hive table used to query it.
   * @param tableName the name of the Hive table
   * @param databaseName the database name where that table is located
   * @return a string of the form "databaseName.tableName"
   */
  def getConcatenatedName(tableName: String, databaseName: Option[String]): String = {
    {
      databaseName match {
        case Some(s) => s + "."
        case None => ""
      }
    } + tableName
  }
}

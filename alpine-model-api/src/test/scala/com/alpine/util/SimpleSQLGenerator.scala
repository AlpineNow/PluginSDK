/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.util

import com.alpine.sql.{DatabaseType, SQLGenerator}

class SimpleSQLGenerator extends SQLGenerator {
  override val quoteChar: Char = '"'
  override def useAliasForSelectSubQueries: Boolean = true
  // TODO: Should escape internal quotes.
  override def escapeColumnName(s: String): String = quoteChar + s + quoteChar

  override def dbType = DatabaseType.postgres
}

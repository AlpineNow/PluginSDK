/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.sql

trait SQLGenerator {
  def dbType: DatabaseType.TypeValue

  def quoteChar: Char

  def useAliasForSelectSubQueries: Boolean

  /**
    * Wraps a column name in the appropriate quote character to preserver case and special characters.
    * If there is no quoting mechanism for this database type, just return the argument.
    *
    * @param s name to be escaped.
    * @return
    */
  def escapeColumnName(s: String): String
}

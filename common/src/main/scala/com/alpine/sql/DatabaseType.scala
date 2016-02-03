/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.sql

object DatabaseType {

  case class TypeValue(name: String)

  /**
    * Note that this may not be a complete list, it's just a list of the already known types.
    */
  val postgres = TypeValue("postgres")
  val oracle = TypeValue("oracle")
  val greenplum = TypeValue("greenplum")
  val hawq = TypeValue("hawq")
  val teradata = TypeValue("teradata")
  val sqlserver = TypeValue("sqlserver")
  val vertica = TypeValue("vertica")
  val mariadb = TypeValue("mariadb")
  val mysql = TypeValue("mysql")
  val hive = TypeValue("hive")
}

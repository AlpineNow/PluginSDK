/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.sql

object DatabaseType {

  case class TypeValue(name: String)

  /**
    * Note that this may not be a complete list, it's just a list of the already known types.
    *
    * Strings should be all lower-case, as in the JDBC URL schemes.
    */

  val PostgresName   = "postgres"
  val PostgreSQLName = "postgresql" // not used, except in the JDBC URL scheme
  val OracleName     = "oracle"
  val GreenplumName  = "greenplum"
  val HAWQName       = "hawq"
  val TeradataName   = "teradata"
  val SQLServerName  = "sqlserver"
  val VerticaName    = "vertica"
  val MariaDBName    = "mariadb"
  val MySQLName      = "mysql"
  val HiveName       = "hive"
  val Hive2Name      = "hive2"
  val SybaseName     = "sybase"
  val DB2Name        = "db2"
  val MongoName      = "mongo"
  val CassandraName  = "cassandra"
  val BigQueryName   = "bigquery"

  val postgres  = TypeValue(PostgresName) // There is no TypeValue for PostgreSQLName
  val oracle    = TypeValue(OracleName)
  val greenplum = TypeValue(GreenplumName)
  val hawq      = TypeValue(HAWQName)
  val teradata  = TypeValue(TeradataName)
  val sqlserver = TypeValue(SQLServerName)
  val vertica   = TypeValue(VerticaName)
  val mariadb   = TypeValue(MariaDBName)
  val mysql     = TypeValue(MySQLName)
  val hive      = TypeValue(HiveName)
  val hive2     = TypeValue(Hive2Name)
  val sybase    = TypeValue(SybaseName)
  val db2       = TypeValue(DB2Name)
  val mongo     = TypeValue(MongoName)
  val cassandra = TypeValue(CassandraName)
  val bigquery  = TypeValue(BigQueryName)

  // All key entries in the map should be lower-case
  // When we search for entries in the map, we first cast keys to lower-case
  val dbTypeMap = Map(
    PostgresName   -> postgres,
    PostgreSQLName -> postgres,
    OracleName     -> oracle,
    GreenplumName  -> greenplum,
    HAWQName       -> hawq,
    TeradataName   -> teradata,
    SQLServerName  -> sqlserver,
    VerticaName    -> vertica,
    MariaDBName    -> mariadb,
    MySQLName      -> mysql,
    HiveName       -> hive,
    Hive2Name      -> hive2,
    SybaseName     -> sybase,
    DB2Name        -> db2,
    MongoName      -> mongo,
    CassandraName  -> cassandra,
    BigQueryName   -> bigquery
  )
}

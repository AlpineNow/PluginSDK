/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.sql

import java.sql.{Connection, ResultSet}

/**
  * Created by Paul Chang 2016-07-12 (6.1 release)
  * <p>
  * The purpose of SQLExecutor is to provide an object to facilitate the execution of commonly used SQL queries.
  * All DDL (data definition language) methods are prefixed with "ddl...". These are methods that create, modify, or
  * drop database objects (tables, views, etc.).
  * <p>
  * Each SQLExecutor is instantiated with a SQLGenerator, URL, and JDBC Connection. The SQLGenerator is used to
  * generate SQL, and the Connection is used to execute SQL. The URL is informational. Several of the methods in
  * SQLGenerator are called by corresponding methods in SQLExecutor and have similar signatures, but unlike
  * SQLGenerator, SQLExecutor will actually execute SQL. SQLGenerator objects are database-type specific, so
  * different SQLGenerator objects are instantiated for different database types.
  * <p>
  * Once instantiated, the SQLGenerator, URL, and Connection for a SQLExecutor do not change. Aside from database
  * changes, this object has no side-effects once instantiated.
  * <p>
  * For certain methods, when specifying table names, fully qualify and delimit as necessary
  * (i.e. "schemaname"."tablename"). Delimiting is required for table names with non-standard characters
  * ([A-Za-z][A-Za-z0-9]*). Not all databases support delimiters or non-standard characters (such as Teradata).
  * For those databases that support schemas, specifying the schema name is recommended.
  * <p>
  * For certain methods, when specifying column names, these must be a comma-separated string of columns
  * that one would find in a SELECT query. This can include expressions that are aliased
  * (i.e. sqlexpression AS aliasname). Any columns with non-standard characters should be delimited
  * (i.e. "columnname"). When SELECTing from more than one table, if column names are not unique,
  * then columns should be fully qualified (i.e. "schemaname"."tablename"."columnname" AS "columnalias").
  * <p>
  * For certain methods, when specifying a source table, it is permissible to specify more than one table as a join
  * (i.e. table1 INNER JOIN table2 ON ...).
  * <p>
  * For certain methods, when specifying a whereClause, the literal "WHERE" should be included.
  * Note that we can include anything that follows a FROM clause here, such as GROUP BY, ORDER BY, etc.
  */
trait SQLExecutor {

  /**
    * Returns the SQLGenerator object used by this SQLExecutor to generate SQL strings. SQLGenerator objects are
    * database-type specific, so different SQLGenerator objects are instantiated for different database types.
    *
    * @return the SQLGenerator object that is embedded within this SQLExecutor.
    */
  def getSQLGenerator: SQLGenerator

  /**
    * Returns the JDBC URL that was used to create the JDBC Connection object that is embedded within this
    * SQLExecutor. The URL is useful in determining the type of the database and other connection information,
    * but does not include security credentials.
    *
    * @return the URL that was used to create the JDBC Connection object embedded within this SQLExecutor.
    */
  def getURL: String

  /**
    * Returns JDBC Connection object embedded within this SQLExecutor. This is also the same Connection that this
    * SQLExecutor uses to execute SQL.
    *
    * @return the JDBC Connection object embedded within this SQLExecutor.
    */
  def getConnection: Connection

  //
  // DDL Methods
  //

  /**
    * Drops table if it exists
    * <p>
    * Note: Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    *
    * @param tableName name of the table to be dropped
    */
  def ddlDropTableIfExists(tableName: String)

  /**
    * Drops table if it exists, optionally cascade drop all dependent objects
    * <p>
    * Note: Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * <p>
    * Note: Not all databases support cascade drop. In such cases, a DROP TABLE without a CASCADE is attempted.
    *
    * @param tableName   name of the table to be dropped
    * @param cascadeFlag if true, drop any dependent objects as well (not supported on all databases)
    */
  def ddlDropTableIfExists(tableName: String, cascadeFlag: Boolean)

  /**
    * Drops view if it exists
    * <p>
    * Note: View name should be fully qualified and delimited, if necessary (i.e. "schemaname"."viewname").
    *
    * @param viewName name of the view to be dropped
    */
  def ddlDropViewIfExists(viewName: String)

  /**
    * Drops view if it exists, optionally cascade drop all dependent objects
    * <p>
    * Note: View name should be fully qualified and delimited, if necessary (i.e. "schemaname"."viewname").
    * <p>
    * Note: Not all databases support cascade drop. In such cases, a DROP VIEW without a CASCADE is attempted.
    *
    * @param viewName    name of the view to be dropped
    * @param cascadeFlag if true, drop any dependent objects as well (not supported on all databases)
    */
  def ddlDropViewIfExists(viewName: String, cascadeFlag: Boolean)

  /**
    * Drops table or view if it exists.
    * SQLExecutor attempts to drop a table with the given name, and then a view with the given name
    * <p>
    * Note: Table or view name should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    *
    * @param tableOrViewName name of the table or view to be dropped
    */
  def ddlDropTableOrViewIfExists(tableOrViewName: String)

  /**
    * Drops table or view if it exists, optionally cascade drop all dependent objects.
    * SQLExecutor attempts to drop a table with the given name, and then a view with the given name
    * <p>
    * Note: Table or view name should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * <p>
    * Note: Not all databases support cascade drop. In such cases, a DROP TABLE or DROP VIEW
    * without a CASCADE is attempted.
    *
    * @param tableOrViewName name of the table or view to be dropped
    * @param cascadeFlag     if true, drop any dependent objects as well (not supported on all databases)
    */
  def ddlDropTableOrViewIfExists(tableOrViewName: String, cascadeFlag: Boolean)

  /**
    * Generates a table based on a SELECT query from the specified table and column list.
    * <p>
    * Table names should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns          String of comma-separated columns to be SELECTed
    * @param sourceTable      name of source table from which we SELECT
    * @param destinationTable name of destination table to be created from SELECT query
    */
  def ddlCreateTableAsSelect(columns: String, sourceTable: String, destinationTable: String)

  /**
    * Generates a table based on a SELECT query from the specified table, column list, and where clause.
    * <p>
    * Table names should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    * <p>
    * The specified WHERE clause should include the "WHERE" keyword. Note that this is simply SQL that
    * is appended to the query as "SELECT columns FROM sourceTable whereClause" and can include other
    * SQL outside of just a WHERE clause, such as GROUP BY, ORDER BY, etc.
    *
    * @param columns          String of comma-separated columns to be SELECTed
    * @param sourceTable      name of source table from which we SELECT
    * @param destinationTable name of destination table to be created from SELECT query
    * @param whereClause      where clause of the SELECT query, including the literal "WHERE" keyword
    */
  def ddlCreateTableAsSelect(columns: String, sourceTable: String, destinationTable: String, whereClause: String)

  /**
    * Generates a table based on a given SELECT query, not necessarily from any particular table.
    * The entire SELECT query must be supplied to generate something like
    * "CREATE TABLE destinationTable AS selectQuery".
    * This method is useful for calls to UDFs or stored procedures that might be database-specific
    * and don't conform to selecting columns from a source table.
    * <p>
    * NOTE: Currently, this works only on certain databases like PostgreSQL, Greenplum, Oracle, MySQL, Teradata,
    * but not MSSQL, which does not support CREATE TABLE ... AS SELECT ....
    * <p>
    * Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * <p>
    * selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
    *
    * @param selectQuery      query (not necessary SELECT) to be used for CREATE TABLE
    * @param destinationTable name of destination table to be created from SELECT query
    */
  def ddlCreateTableAsSelect(selectQuery: String, destinationTable: String)

  /**
    * Generates a view based on a SELECT query from the specified table and column list.
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns         String of comma-separated columns to be SELECTed
    * @param sourceTable     name of source table from which we SELECT
    * @param destinationView name of destination view to be created from SELECT query
		*/
  def ddlCreateViewAsSelect(columns: String, sourceTable: String, destinationView: String)

  /**
    * Generates a view based on a SELECT query from the specified table, column list, and where clause.
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    * <p>
    * The specified WHERE clause should include the "WHERE" keyword. Note that this is simply SQL that
    * is appended to the query as "SELECT columns FROM sourceTable whereClause" and can include other
    * SQL outside of just a WHERE clause, such as GROUP BY, ORDER BY, etc.
    *
    * @param columns         String of comma-separated columns to be SELECTed
    * @param sourceTable     name of source table from which we SELECT
    * @param destinationView name of destination view to be created from SELECT query
    * @param whereClause      where clause of the SELECT query, including the literal "WHERE" keyword
    */
  def ddlCreateViewAsSelect(columns: String, sourceTable: String, destinationView: String, whereClause: String)

  /**
    * Generates a view based on a given SELECT query, not necessarily from any particular table.
    * The entire SELECT query must be supplied to generate something like
    * "CREATE VIEW destinationTable AS selectQuery".
    * This method is useful for calls to UDFs or stored procedures that might be database-specific
    * and don't conform to selecting columns from a source table.
    * <p>
    * Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * <p>
    * selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
    *
    * @param selectQuery     query (not necessary SELECT) to be used for CREATE VIEW
    * @param destinationView name of destination table to be created from SELECT query
    */
  def ddlCreateViewAsSelect(selectQuery: String, destinationView: String)

  /**
    * Generates a table or view based on a SELECT query from an existing table and column list.
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns                String of comma-separated columns to be SELECTed
    * @param sourceTable            name of source table from which we SELECT
    * @param destinationTableOrView name of destination table or view to be created from SELECT query
    * @param isView                 true if we are generating a view, false if we are generating a table
    */
  def ddlCreateTableOrViewAsSelect(columns: String, sourceTable: String, destinationTableOrView: String, isView: Boolean)

  /**
    * Generates a table or view based on a SELECT query from an existing table, column list, and where clause.
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * It is acceptable to specify a table join as the sourceTable (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    * <p>
    * The specified WHERE clause should include the "WHERE" keyword. Note that this is simply SQL that
    * is appended to the query as "SELECT columns FROM sourceTable whereClause" and can include other
    * SQL outside of just a WHERE clause, such as GROUP BY, ORDER BY, etc.
    *
    * @param columns                String of comma-separated columns to be SELECTed
    * @param sourceTable            name of source table from which we SELECT
    * @param destinationTableOrView name of destination table or view to be created from SELECT query
    * @param whereClause            where clause of the SELECT query, including the literal "WHERE" keyword
    * @param isView                 true if we are generating a view, false if we are generating a table
    */
  def ddlCreateTableOrViewAsSelect(columns: String, sourceTable: String, destinationTableOrView: String, whereClause: String, isView: Boolean)

  /**
    * Generates a table or view based on a given SELECT query, not necessarily from any particular table.
    * The entire SELECT query must be supplied to generate something like
    * "CREATE TABLE destinationTable AS selectQuery" or "CREATE VIEW destinationView AS selectQuery.
    * This method is useful for calls to UDFs or stored procedures that might be database-specific
    * and don't conform to selecting columns from a source table.
    * <p>
    * NOTE: Currently, table creation does not work on MSSQL, which does not support
    * CREATE TABLE ... AS SELECT ....
    * <p>
    * Table or view name should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * <p>
    * selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
    *
    * @param selectQuery            query (not necessary SELECT) to be used for CREATE TABLE
    * @param destinationTableOrView name of destination table to be created from SELECT query
    * @param isView                 true if we are generating a view, false if we are generating a table
    */
  def ddlCreateTableOrViewAsSelect(selectQuery: String, destinationTableOrView: String, isView: Boolean)

  /**
    * Checks if table or view exists, and if so, returns true.
    *
    * @param tableOrViewName name of table or view to check
    * @return                true if the table or view does exist, false otherwise
    */
  def tableOrViewExists(tableOrViewName: String): Boolean

  /**
    * Returns number of rows from a table or view.
    *
    * @param tableOrViewName name of table or view for which to count rows
    * @return                number of rows found
    */
  def getRowCount(tableOrViewName: String): Long

  /**
    * Executes a DML-like query that INSERTs, UPDATEs, or DELETEs rows.
    *
    * @param sql DML query to execute
    * @return number of rows affected or 0
    */
  def executeUpdate(sql: String): Int

  /**
    * Executes a query that returns a ResultSet and applies the function to it.
    *
    * @param sql query to execute, assumes a single ResultSet will be generated from it.
    * @param resultSetParser function that takes a ResultSet and returns something
    * @tparam R Type parameter returned by resultSetParser
    * @return Invocation of resultSetParser on ResultSet, returns object of data type R
    */
  def executeQuery[R](sql: String, resultSetParser: (ResultSet => R)): R

  /**
    * Executes a query that returns a ResultSet and applies the ResultSetParser object to it.
    *
    * @param sql query to execute, assumes a single ResultSet will be generated from it.
    * @param resultSetParser SQLExecutorResultSetParser object that takes a ResultSet and parses it to return something
    * @tparam R Type parameter returned by SQLExecutorResultSetParser
    * @return Invocation of resultSetParser on ResultSet, returns object of data type R
    */
  def executeQuery[R](sql: String, resultSetParser: SQLExecutorResultSetParser[R]): R

  /**
    * Executes a query that returns a ResultSet, but transforms it into an array of arrays of Objects.
    *
    * @param sql query to execute, assumes a single ResultSet will be generated from it.
    * @return Array of Array of Objects, transformed from ResultSet
    */
  def executeQuery(sql: String): Array[Array[Object]]
}

/**
  * Helper interface for Java to parse ResultSet objects. For Scala, we simply use
  * executeQuery[R](String, (ResultSet => R)): R
  *
  * For Java, passing lambda functions is more complicated until Java 8.
  * So instead, we have a helper interface. Create an object that will parse the ResultSet
  * and pass it to SQLExecutor.executeQuery().
  */
trait SQLExecutorResultSetParser[R] {
  def parseResultSet(rs: ResultSet): R
}

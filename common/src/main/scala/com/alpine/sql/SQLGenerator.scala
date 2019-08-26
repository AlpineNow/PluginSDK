/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.sql

/**
  * The purpose of SQLGenerator is to provide an object to facilitate the construction of commonly used SQL queries.
  * Because SQL generation can vary between different database types, each SQLGenerator is instantiated for a
  * particular database type.
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
trait SQLGenerator {

	//
	// Database information methods
	//

  /**
    * Returns the DatabaseType.TypeValue object that represents the database type. Each SQLGenerator is
    * instantiated for a particular database type.
    *
    * @return the DatabaseType.TypeValue object that represents the database type of this SQLGenerator
    */
  def dbType: DatabaseType.TypeValue

	//
	// SQL syntax information methods
	//

  /**
    * Character or String to use when quoting identifiers.
    * Typically, this is a double-quote (for PostgreSQL and other databases), but can be other characters
    * or even Strings for other platforms.
    *
    * @return the character or characters to be used to delimit identifiers for this database type
    */
  def quoteChar: String

  /**
		* If true, subqueries must be aliased. Some databases, such as Oracle, require subqueries
    * (queries that appear in the FROM clause) to be aliased. For example:
    *
    * SELECT ...
    * FROM (
    *     SELECT ...
    *     FROM foo
    *     WHERE ...
    *     ) AS fooalias
    * WHERE ...
    *
    * @return true if subqueries must be aliased, false if otherwise
		*/
  def useAliasForSelectSubQueries: Boolean

  //
  // Quoting methods
  //

  /**
		* Wraps a column name in the appropriate quote character to preserve case and special characters.
		* If there is no quoting mechanism for this database type, just return the argument.
    * <p>
		* Note: Deprecated -- please use quoteIdentifier instead.
    *
    * @param  s string to be delimited
    * @return the given string, but with appropriate delimiters for the current database type
		*/
  @deprecated("Please use quoteIdentifier instead [Paul]", "2016-04-22")
  def escapeColumnName(s: String): String

  /**
		* Wraps an identifier in the appropriate quote character to preserve case and special characters.
		* If there is no quoting mechanism for this database type, just return the argument.
    *
    * @param  s string to be delimited
    * @return the given string, but with the appropriate delimiters for the current database type
		*/
  def quoteIdentifier(s: String): String

  /**
		* Wraps a schema and table or view name in the appropriate quote character to preserve case and special characters.
		* If schemaName is unspecified, quote only the objectName. Uses quoteIdentifier() for quoting.
    *
    * @param schemaName the schema name of the object (or "" if none)
    * @param objectName the object name of the object
    * @return a string composed of the given schema and object names, properly delimited
		*/
  def quoteObjectName(schemaName: String, objectName: String): String

  //
  // Methods for various SQL functions
  //

  /**
		* Returns the name of the standard deviation function to be used for this database.
		* For example, in PostgreSQL, this is "stddev". Note that this is just the function name,
		* and does not include the argument. A typical SQL call would be "stddev(expression)".
		*
    * @return name of the standard deviation function for the current database type
		*
		* @deprecated the standard deviation function is ambiguous, as it varies on different RDBMS platforms.
		*            Replace with [[getStandardDeviationPopulationFunctionName]]
		*                 or [[getStandardDeviationSampleFunctionName]]
		*/
	@deprecated("use getStandardDeviationPopulationFunctionName or getStandardDeviationSampleFunctionName instead, which is unambiguous", "6.4")
  @Deprecated def getStandardDeviationFunctionName: String

	/**
		* Returns the name of the entire population standard deviation function to be used for this database.
		* This is defined as &sigma;=SQRT((1/N)&Sigma;<sub>i=1</sub><sup>N</sup> (x<sub>i</sub>)-&mu;)<sup>2</sup>).
		* Note that this is a different function from the sample standard deviation function.
		* Different RDBMSs have different names:
		* <br/>
		* PostgreSQL/GreenPlum: STDDEV_POP()
		* MSSQL: STDEVP()
		* MySQL/MariaDB: STD(), STDDEV(), or STDDEV_POP()
		* Oracle: STDDEV_POP()
		* Teradata: STDDEV_POP()
		* Hive: STDDEV_POP()
		* DB2: STDDEV()
		* Sybase: STDDEV_POP()
		*
		* @return name of the standard deviation population function for the current database type
		*/
	def getStandardDeviationPopulationFunctionName: String

	/**
		* Returns the name of the sample standard deviation function to be used for this database.
		* This is defined as s=SQRT((1/(n-1))&Sigma;<sub>i=1</sub><sup>N</sup> (x<sub>i</sub>-x&#x2c9;)<sup>2</sup>).
		* Note that this is a different function from the entire population standard deviation function.
		* Different RDBMSs have different names:
		* <br/>
		* PostgreSQL/GreenPlum: STDDEV_SAMP()
		* MSSQL: STDEV()
		* MySQL/MariaDB: STDDEV_SAMP()
		* Oracle: STDDEV_SAMP(), STDDEV()
		* Teradata: STDDEV_SAMP()
		* Hive: STDDEV_SAMP()
		* DB2: STDDEV_SAMP()
		* Sybase: STDDEV_SAMP(), STDDEVIATION(), STDDEV()
		*
		* @return name of the standard deviation population function for the current database type
		*/
	def getStandardDeviationSampleFunctionName: String

  /**
		* Returns the name of the variance function to be used for this database.
		* For example, in PostgreSQL, this is "variance". Note that this is just the function name,
		* and does not include the argument. A typical call would be "variance(expression)".
    *
    * @return name of the variance function for the current database type
		*
		* @deprecated the variance function is ambiguous, as it varies on different RDBMS platforms.
		*            Replace with [[getVariancePopulationFunctionName]]
		*                 or [[getVarianceSampleFunctionName]]
		*/
	@deprecated("use getVariancePopulationFunctionName or getVarianceSampleFunctionName instead, which is unambiguous", "6.4")
  @Deprecated def getVarianceFunctionName: String

	/**
		* Returns the name of the entire population standard deviation function to be used for this database.
		* This is defined as &sigma;<sup>2</sup>=(1/N)&Sigma;<sub>i=1</sub><sup>N</sup> (x<sub>i</sub>)-&mu;)<sup>2</sup>.
		* Note that this is a different function from the sample standard deviation function.
		* Different RDBMSs have different names:
		* <br/>
		* PostgreSQL/GreenPlum: VAR_POP()
		* MSSQL: VARP()
		* MySQL/MariaDB: VARIANCE(), VAR_POP()
		* Oracle: VAR_POP()
		* Teradata: VAR_POP()
		* Hive: VARIANCE(), VAR_POP()
		* DB2: VARIANCE()
		* Sybase: VAR_POP()
		*
		* @return name of the standard deviation population function for the current database type
		*/
	def getVariancePopulationFunctionName: String

	/**
		* Returns the name of the sample variance function to be used for this database.
		* This is defined as s<sup>2</sup>=(1/(n-1))&Sigma;(i=1..n (x<sub>i</sub>)-x&#x2c9;))<sup>2</sup>).
		* Note that this is a different function from the entire population standard deviation function.
		* Different RDBMSs have different names:
		* <br/>
		* PostgreSQL/GreenPlum: , "VAR_POP"(), VARIANCE()
		* MSSQL: VAR()
		* MySQL/MariaDB: VAR_SAMP()
		* Oracle: VAR_SAMP(), VARIANCE()
		* Teradata: VAR_SAMP()
		* Hive: VAR_SAMP()
		* DB2: VARIANCE_SAMP()
		* Sybase: VAR_SAMP()
		*
		* @return name of the standard deviation population function for the current database type
		*/
	def getVarianceSampleFunctionName: String

	/**
		* Returns the expression for the correlation function to be used for this database.
		* Most RDBMSs have a CORR(X,Y) function, but some do not.
		* For those that do not have a built-in function, we substitute it with the expression
		* (AVG(XY)-AVG(X)AVG(Y))/(STDDEV_POP(X)*STDDEV_POP(Y)) where both X and Y are NOT NULL.
		*
		* @param columnX expression used for column X, typically a column name
		* @param columnY expression used for column Y, typically a column name
		* @return an expression returning the Pearson correlation coefficient, typically CORR(columnX, columnY)
		*/
	def getCorrelationExpression(columnX: String, columnY: String): String

	/**
		* Returns the expression for modulo division for this database. This differs widely among databases.
		* For example, in PostgreSQL, this is "dividend % divisor".
		* But in Oracle, this is "MODULO(dividend, divisor)".
		* For this reason, we must pass in the expressions used for dividend and divisor.
    *
    * @param dividend SQL expression for the dividend to be passed to the MODULO (can be a simple column name)
    * @param divisor SQL expression for the divisor to be passed to the MODULO (can be a simple number)
    * @return a SQL expression for the given dividend modulo the given divisor
		*/
  def getModuloExpression(dividend: String, divisor: String): String

  //
  // Methods to generate SQL DDL Statements
  //

  /**
		* Returns a SQL DDL statement to generate a table based on a SELECT query from an existing table.
		* For most databases, this is something like
    * "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable whereClause"
    * <p>
    * Table names should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
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
    * @return                 generated SQL statement
		*/
  def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String

  /**
		* Returns a SQL DDL statement to generate a table based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable"
    * <p>
    * Table names should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns          String of comma-separated columns to be SELECTed
    * @param sourceTable      name of source table from which we SELECT
    * @param destinationTable name of destination table to be created from SELECT query
    * @return                 generated SQL statement
		*/
  def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String): String

  /**
		* Returns a SQL DDL statement to generate a view based on a SELECT query from an existing table.
		* For most databases, this is something like
    * "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable [whereClause]"
    * <p>
    * View names should be fully qualified and delimited, if necessary (i.e. "schemaname"."viewname").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
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
    * @param whereClause     where clause of the SELECT query, including the literal "WHERE" keyword
    * @return                generated SQL statement
		*/
  def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String, whereClause: String): String

  /**
		* Returns a SQL DDL statement to generate a view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable"
    * <p>
    * View names should be fully qualified and delimited, if necessary (i.e. "schemaname"."viewname").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns         String of comma-separated columns to be SELECTed
    * @param sourceTable     name of source table from which we SELECT
    * @param destinationView name of destination view to be created from SELECT query
    * @return                generated SQL statement
		*/
  def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String): String

	/**
		* Returns a SQL DDL statement to generate a table or view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable whereClause"
		* or "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable whereClause", depending on the isView parameter
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename", "schemaname"."viewname").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    * <p>
    * The specified WHERE clause should include the "WHERE" keyword. Note that this is simply SQL that
    * is appended to the query as "SELECT columns FROM sourceTable whereClause" and can include other
    * SQL outside of just a WHERE clause, such as GROUP BY, ORDER BY, etc.
    *
    * @param columns          String of comma-separated columns to be SELECTed
    * @param sourceTable      name of source table from which we SELECT
    * @param destinationTable name of destination table or view to be created from SELECT query
    * @param whereClause      where clause of the SELECT query, including the literal "WHERE" keyword
    * @param isView           true if generating a view, false if generating a table
    * @return                 generated SQL statement
		*/
	def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String, isView: Boolean): String

	/**
		* Returns a SQL DDL statement to generate a table or view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable"
		* or "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable", depending on the isView parameter
    * <p>
    * Table and view names should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename", "schemaname"."viewname").
    * it is acceptable to specify a table join as the source table (i.e. table1 INNER JOIN table2 ...),
    * but in such cases, column names might not be unique and full qualification and aliasing may be
    * required (i.e. "schemaname"."tablename"."columnname" AS "aliasname")
    *
    * @param columns          String of comma-separated columns to be SELECTed
    * @param sourceTable      name of source table from which we SELECT
    * @param destinationTable name of destination table or view to be created from SELECT query
    * @param isView           true if generating a view, false if generating a table
    * @return                 generated SQL statement
		*/
	def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, isView: Boolean): String

  /**
    * Returns a SQL DDL statement to generate a table based on a SELECT query,
    * not necessarily from any particular table. The entire SELECT query must be supplied
    * to generate something like "CREATE TABLE destinationTable AS selectQuery".
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
    * @return                 generated SQL statement
    */
  def getCreateTableAsSelectSQL(selectQuery: String, destinationTable: String): String

  /**
    * Returns a SQL DDL statement to generate a view based on a SELECT query,
    * not necessarily from any particular table. The entire SELECT query must be supplied
    * to generate something like "CREATE VIEW destinationTable AS selectQuery".
    * This method is useful for calls to UDFs or stored procedures that might be database-specific
    * and don't conform to selecting columns from a source table.
    * <p>
    * Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
    * <p>
    * selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
    *
    * @param selectQuery     query (not necessary SELECT) to be used for CREATE VIEW
    * @param destinationView name of destination table to be created from SELECT query
    * @return                 generated SQL statement
    */
  def getCreateViewAsSelectSQL(selectQuery: String, destinationView: String): String

  /**
    * Returns a SQL DDL statement to generate a table or view based on a SELECT query,
    * not necessarily from any particular table. The entire SELECT query must be supplied
    * to generate something like "CREATE TABLE destinationTable AS selectQuery"
    * or "CREATE VIEW destinationTable AS selectQuery", depending on the isView parameter.
    * This method is useful for calls to UDFs or stored procedures that might be database-specific
    * and don't conform to selecting columns from a source table.
    * <p>
    * NOTE: Currently, creating a table works only on certain databases but not MSSQL,
    * which does not support CREATE TABLE ... AS SELECT ....
    * <p>
    * Table or view name should be fully qualified and delimited, if necessary
    * (i.e. "schemaname"."tablename" or "schemaname"."viewname").
    * <p>
    * selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
    *
    * @param selectQuery            query (not necessary SELECT) to be used for CREATE TABLE
    * @param destinationTableOrView name of destination table to be created from SELECT query
    * @return                       generated SQL statement
    */
  def getCreateTableOrViewAsSelectSQL(selectQuery: String, destinationTableOrView: String, isView: Boolean): String

  /**
	  * Returns a SQL DDL statement to drop a table if it exists, and optionally, cascade the drop to dependent tables.
	  * For most databases, this is something like "DROP TABLE IF EXISTS tableName [CASCADE]"
    * <p>
    * NOTE: Not all databases support CASCADE DROP
    *
    * @param tableName table to be dropped
    * @param cascade   true if dependent objects should also be dropped
    * @return          generated SQL statement
    */
  def getDropTableIfExistsSQL(tableName: String, cascade: Boolean = false): String

	/**
	  * Returns a SQL DDL statement to drop a view if it exists, and optionally, cascade the drop to dependent views.
	  * For most databases, this is something like "DROP VIEW IF EXISTS viewName [CASCADE]"
    * <p>
    * NOTE: Not all databases support CASCADE DROP
    *
    * @param viewName view to be dropped
    * @param cascade  true if dependent objects should also be dropped
    * @return         generated SQL statement
	  */
  def getDropViewIfExistsSQL(viewName: String, cascade: Boolean = false): String

	/**
		* Converts a double to String representation. For most database vendors, this is just invoking toString on the double
		* object. For Teradata, this will format the number of digits to no more than 15.
		*
		* @param d the double
		* @return String representation of the double, limited to 15 characters for Teradata
		*/
	def doubleToString(d: Double): String = {
		d.toString
	}

	/**
		* Returns a SQL DDL statement to generate a temporary table based on a SELECT query,
		* not necessarily from any particular table. The entire SELECT query must be supplied
		* to generate something like "CREATE TEMP TABLE destinationTable AS selectQuery".
		* This method is useful for calls to UDFs or stored procedures that might be database-specific
		* and don't conform to selecting columns from a source table.
		* <p>
		* NOTE: Currently, this works only on certain databases that support temporary tables.
		* <p>
		* Table name should be fully qualified and delimited, if necessary (i.e. "schemaname"."tablename").
		* <p>
		* selectQuery is not necessarily a SELECT query, but any query that can feed the CREATE TABLE statement.
		*
		* @param selectQuery      query (not necessary SELECT) to be used for CREATE TABLE
		* @param destinationTable name of destination table to be created from SELECT query
		* @return generated SQL statement
		*/
	def getCreateTempTableAsSelectSQL(selectQuery: String, destinationTable: String): String

	/**
		* Should one put quotes around boolean values
		*
		* introduced in 2019 mainly to support BigQuery
		*
		* @return Boolean telling whether or not to put quotes around Boollean values
		*/
	def quoteBooleanValues(): Boolean

	/**
		* Convert strings read from table into boolean
		*
		* introduced in 2019 mainly to support BigQuery
		*
		* @return Boolean corresponding to the string value
		*/
	def convertStringToBoolean(input : String): Boolean

}

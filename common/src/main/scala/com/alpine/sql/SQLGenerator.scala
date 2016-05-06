/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.sql

trait SQLGenerator {

	//
	// Database information methods
	//

  /**
    * Returns the DatabaseType.TypeValue object that represents the database type.
    *
    * @return
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
    * @return
    */
  def quoteChar: String

  /**
		* If true, subqueries must be aliased.
		*
		* @return
		*/
  def useAliasForSelectSubQueries: Boolean

  //
  // Quoting methods
  //

  /**
		* Wraps a column name in the appropriate quote character to preserve case and special characters.
		* If there is no quoting mechanism for this database type, just return the argument.
		* Note: Deprecated -- please use quoteIdentifier instead.
		*
		* @param s name to be escaped.
		* @return
		*/
  @deprecated("Please use quoteIdentifier instead [Paul]", "2016-04-22")
  def escapeColumnName(s: String): String

  /**
		* Wraps an identifier in the appropriate quote character to preserve case and special characters.
		* If there is no quoting mechanism for this database type, just return the argument.
		*
		* @param s identifier name to be quoted
		* @return
		*/
  def quoteIdentifier(s: String): String

  /**
		* Wraps a schema and table or view name in the appropriate quote character to preserve case and special characters.
		* If schemaName is unspecified, quote only the objectName. Uses quoteIdentifier() for quoting.
		*
		* @param schemaName
		* @param objectName
		* @return
		*/
  def quoteObjectName(schemaName: String, objectName: String): String

  //
  // Methods for various SQL functions
  //

  /**
		* Returns the name of the standard deviation function to be used for this database.
		* For example, in PostgreSQL, this is "stddev". Note that this is just the function name,
		* and does not include the argument. A typical call would be "stddev(expression)".
		*
		* @return
		*/
  def getStandardDeviationFunctionName: String

  /**
		* Returns the name of the variance function to be used for this database.
		* For example, in PostgreSQL, this is "variance". Note that this is just the function name,
		* and does not include the argument. A typical call would be "variance(expression)".
		*
		* @return
		*/
  def getVarianceFunctionName: String

  /**
		* Returns the expression for modulo division for this database. This differs widely among databases.
		* For example, in PostgreSQL, this is "dividend % divisor".
		* But in Oracle, this is "MODULO(dividend, divisor)".
		* For this reason, we must pass in the expressions used for dividend and divisor.
		*
		* @return
		*/
  def getModuloExpression(dividend: String, divisor: String): String

  //
  // Methods to generate SQL DDL Statements
  //

  /**
		* Returns a SQL DDL statement to generate a table based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable whereClause"
		*
		* @param columns
		* @param sourceTable
		* @param destinationTable
		* @param whereClause
		*/
  def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String

  /**
		* Returns a SQL DDL statement to generate a table based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable"
		*
		* @param columns
		* @param sourceTable
		* @param destinationTable
		*/
  def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String): String

  /**
		* Returns a SQL DDL statement to generate a view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable [whereClause]"
		*
		* @param columns
		* @param sourceTable
		* @param destinationView
		* @param whereClause
		*/
  def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String, whereClause: String): String

  /**
		* Returns a SQL DDL statement to generate a view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable"
		*
		* @param columns
		* @param sourceTable
		* @param destinationView
		*/
  def getCreateViewAsSelectSQL(columns: String, sourceTable: String, destinationView: String): String

	/**
		* Returns a SQL DDL statement to generate a table or view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable whereClause"
		* or "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable whereClause", depending on the isView parameter
		*
		* @param columns
		* @param sourceTable
		* @param destinationTable
		* @param whereClause
		* @param isView
		*/
	def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String, isView: Boolean): String

	/**
		* Returns a SQL DDL statement to generate a table or view based on a SELECT query from an existing table.
		* For most databases, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable"
		* or "CREATE VIEW destinationTable AS SELECT columns FROM sourceTable", depending on the isView parameter
		*
		* @param columns
		* @param sourceTable
		* @param destinationTable
		* @param isView
		*/
	def getCreateTableOrViewAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, isView: Boolean): String

	/**
	  * Returns a SQL DDL statement to drop a table if it exists, and optionally, cascade the drop to dependent tables.
	  * For most databases, this is something like "DROP TABLE IF EXISTS tableName [CASCADE]"
  	*
	  * @param tableName
	  * @param cascade
	  * @return
	  */
  def getDropTableIfExistsSQL(tableName: String, cascade: Boolean = false): String

	/**
	  * Returns a SQL DDL statement to drop a view if it exists, and optionally, cascade the drop to dependent views.
	  * For most databases, this is something like "DROP VIEW IF EXISTS viewName [CASCADE]"
	  *
	  * @param viewName
	  * @param cascade
	  * @return
	  */
  def getDropViewIfExistsSQL(viewName: String, cascade: Boolean = false): String
}

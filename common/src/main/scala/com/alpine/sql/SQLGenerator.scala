/*
 * COPYRIGHT (C) Jan 26 2016 Alpine Data Labs Inc. All Rights Reserved.
 */
package com.alpine.sql

trait SQLGenerator {

      /**
        * Returns the DatabaseType.TypeValue object that represents the RDBMS type.
        *
        * @return
        */
      def dbType: DatabaseType.TypeValue

      /**
        * Character or String to use when quoting identifiers.
        * Typically, this is a double-quote (for PostgreSQL and other RDBMSs), but can be other characters
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
        * Returns the name of the standard deviation function to be used for this RDBMS.
        * For example, in PostgreSQL, this is "stddev". Note that this is just the function name,
        * and does not include the argument. A typical call would be "stddev(expression)".
        *
        * @return
        */
      def getStandardDeviationFunctionName: String

      /**
        * Returns the name of the variance function to be used for this RDBMS.
        * For example, in PostgreSQL, this is "variance". Note that this is just the function name,
        * and does not include the argument. A typical call would be "variance(expression)".
        *
        * @return
        */
      def getVarianceFunctionName: String

      /**
        * Returns the expression for modulo division for this RDBMS. This differs widely among RDBMSs.
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
        * For most RDBMSs, this is something like "CREATE TABLE destinationTable AS SELECT columns FROM sourceTable [whereClause]"
        *
        * @param columns
        * @param sourceTable
        * @param destinationTable
        * @param whereClause
        */
      def getCreateTableAsSelectSQL(columns: String, sourceTable: String, destinationTable: String, whereClause: String): String
}

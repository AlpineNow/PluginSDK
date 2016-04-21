package com.alpine.plugin.core.utils

/**
  * Created by mthyen on 4/5/16.
  */
object DBSQLUtils {

    /**
      * Given an identifier and an optional quoteChar, will return the identifier enclosed by the quoteChar. If the
      * identifier is already enclosed by the quoteChar, this will return the identifier. For example,
      * calling quote("foo") will return "\"foo\"". Calling quote ("\"foo\"") will return "\"foo\"". In SQL sent to the database,
      * this will evaluate to "foo". This is intended for a database system that needs identifiers to be quoted for capitalization
      * and special characters. The default value for quoteChar is double quote (") but this method supports other values including
      * a blank value (for Hive JDBC).
      *
      * @param identifier the value to be enclosed in quotes if quotes are necessary
      * @param quoteChar the quotation character, default is double quote (")
      * @return returns the identifier enclosed in the quotes if the identifier is not already enclosed in quotes
      */
    def quote(identifier: String, quoteChar: String = "\""): String = {
        // builds a regex like (")(.+)(") to match a string that begins and ends with quotes
        // should match "foo"
        // but not match foo (without quotes)
        def getPattern(quoteChar: String): scala.util.matching.Regex = {
            val patternString = "(" + quoteChar + ")" + "(.+)" + "(" + quoteChar + ")"
            patternString.r // Note the .r at the end. This returns a Regex
        }

        // this does the actual quoting to prepend and append the quoteChar to the identifier
        def quoter(identifier: String, quoteChar: String): String = {
            quoteChar + identifier + quoteChar
        }

        val pattern = getPattern(quoteChar)
        val result = identifier match {
            // If the incoming identifier is already quoted, return the incoming identifier
            case pattern(begin, middle, end) => identifier
            // If identifier is not quoted add quotes
            case _ => quoter(identifier, quoteChar)
        }
        //println("identifier [" + identifier + "] -> result: [" + result + "] with quoteChar: [" + quoteChar + "]")
        result
    }

    /**
      * Given a schema name, a table name, and an optional quoteChar, will return a quoted string with the fully
      * qualified name with the schema and table name enclosed in quotes. For example, calling
      * quoteSchemaTable("foo", "bar") will return "\"foo\".\"bar\"". In SQL sent to the database, this will evalue
      * to "foo"."bar". This is intended for a database system that needs identifiers to be quoted for capitalization
      * and special characters. The default value for quoteChar is double quote (") but this method supports other values including
      * a blank value (for Hive JDBC).
      *
      * @param schema the schema value to be enclosed in quotes if quotes are necessary
      * @param table the table value to be enclosed in quotes if quotes are necessary
      * @param quoteChar the quotation character, default is double quote (")
      * @return returns "schema"."table" or similar depending upon quoteChar that is used
      */
    def quoteSchemaTable(schema: String, table: String, quoteChar: String = "\""): String = {
        quote(schema, quoteChar) + "." + quote(table, quoteChar)
    }
}

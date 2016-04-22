package com.alpine.plugin.core.utils

import org.scalatest.FunSpec

/**
  * Created by mthyen on 4/5/16.
  */
class DBSQLUtils$Test extends FunSpec {

    import DBSQLUtils._

    describe("testing quote") {
        it("should handle default value for quoteChar") {
            assert(quote("foo") === "\"foo\"")
            assert(quote("\"foo\"") === "\"foo\"")
            assert(quote("`foo`") === "\"`foo`\"")
        }
        it("should handle value passed in for quoteChar") {
            assert(quote("foo", "\"") === "\"foo\"")
            assert(quote("\"foo\"", "\"") === "\"foo\"")
            assert(quote("`foo`", "\"") === "\"`foo`\"")
        }
        it("should handle backtick passed in for quoteChar") {
            assert(quote("foo", "`") === "`foo`")
            assert(quote("\"foo\"", "`") === "`\"foo\"`")
            assert(quote("`foo`", "`") === "`foo`")
        }
        it("should handle empty string passed in for quoteChar") {
            assert(quote("foo", "") === "foo")
            assert(quote("\"foo\"", "") === "\"foo\"")
            assert(quote("`foo`", "") === "`foo`")
        }
    }
    describe ("test quoteSchemaTable") {
        it("should handle default value for quoteChar") {
            assert(quoteSchemaTable("s", "t") === "\"s\".\"t\"")
            assert(quoteSchemaTable("\"s\"", "\"t\"") === "\"s\".\"t\"")
            assert(quoteSchemaTable("`s`", "`t`") === "\"`s`\".\"`t`\"")
        }
        it("should handle value passed in for quoteChar") {
            assert(quoteSchemaTable("s", "t", "\"") === "\"s\".\"t\"")
            assert(quoteSchemaTable("\"s\"", "\"t\"", "\"") === "\"s\".\"t\"")
            assert(quoteSchemaTable("`s`", "`t`", "\"") === "\"`s`\".\"`t`\"")
        }
        it("should handle backtick passed in for quoteChar") {
            assert(quoteSchemaTable("s", "t", "`") === "`s`.`t`")
            assert(quoteSchemaTable("\"s\"", "\"t\"", "`") === "`\"s\"`.`\"t\"`")
            assert(quoteSchemaTable("`s`", "`t`", "`") === "`s`.`t`")
        }
        it("should handle empty string passed in for quoteChar") {
            assert(quoteSchemaTable("s", "t", "") === "s.t")
            assert(quoteSchemaTable("\"s\"", "\"t\"", "") === "\"s\".\"t\"")
            assert(quoteSchemaTable("`s`", "`t`", "") === "`s`.`t`")
        }
    }
}

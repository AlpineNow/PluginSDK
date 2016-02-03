/*
 * COPYRIGHT (C) Jan 28 2016 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.util

import com.alpine.plugin.core.io.{ColumnType, ColumnDef}
import org.scalatest.FunSuite

/**
  * Created by Jennifer Thompson on 1/28/16.
  */
class FeatureUtil$Test extends FunSuite {

  import FeatureUtil._

  test("Should sanitize column names properly.") {
    assert("a" === sanitizeStringForColumnName("a"))
    assert("_1" === sanitizeStringForColumnName("1"))
    assert("apple" === sanitizeStringForColumnName("apples", Some(5)))
    assert("apple" === sanitizeStringForColumnName("apple", Some(5)))
    assert("a" === sanitizeStringForColumnName("a", Some(5)))
    assert("_apples_" === sanitizeStringForColumnName("+apples+"))
    assert("_appl" === sanitizeStringForColumnName("+apples+", Some(5)))
  }

  test("Should deDuplicate column names properly.") {
    assert(
      Seq(ColumnDef("a", ColumnType.Double))
        === deDuplicate(
        Seq(ColumnDef("a", ColumnType.Double))
      )
    )
    assert(
      Seq(
        ColumnDef("a", ColumnType.Double),
        ColumnDef("b", ColumnType.Double)
      )
        === deDuplicate(
        Seq(
          ColumnDef("a", ColumnType.Double),
          ColumnDef("b", ColumnType.Double)
        )
      )
    )
    assert(
      Seq(
        ColumnDef("a", ColumnType.Double),
        ColumnDef("a_1", ColumnType.Double),
        ColumnDef("b", ColumnType.Double)
      )
        === deDuplicate(
        Seq(
          ColumnDef("a", ColumnType.Double),
          ColumnDef("a", ColumnType.Double),
          ColumnDef("b", ColumnType.Double)
        )
      )
    )
    assert(
      Seq(
        ColumnDef("a", ColumnType.Double),
        ColumnDef("a_1", ColumnType.String),
        ColumnDef("b", ColumnType.Double)
      )
        === deDuplicate(
        Seq(
          ColumnDef("a", ColumnType.Double),
          ColumnDef("a", ColumnType.String),
          ColumnDef("b", ColumnType.Double)
        )
      )
    )
    assert(
      Seq(
        ColumnDef("a", ColumnType.Double),
        ColumnDef("a_1", ColumnType.String),
        ColumnDef("a_2", ColumnType.Long),
        ColumnDef("b", ColumnType.Double),
        ColumnDef("b_1", ColumnType.String)
      )
        === deDuplicate(
        Seq(
          ColumnDef("a", ColumnType.Double),
          ColumnDef("a", ColumnType.String),
          ColumnDef("a", ColumnType.Long),
          ColumnDef("b", ColumnType.Double),
          ColumnDef("b", ColumnType.String)
        )
      )
    )
  }

  test("Should generate the correct categoricalModelSQLOutputFeatures") {
    assert(
      Seq(
        ColumnDef("PRED", ColumnType.String),
        ColumnDef("DIST_1", ColumnType.Double),
        ColumnDef("DIST_0", ColumnType.Double)
      )
      === categoricalModelSQLOutputFeatures(DIST, Seq("1", "0"))
    )
    assert(
      Seq(
        ColumnDef("PRED", ColumnType.String),
        ColumnDef("DIST_yes", ColumnType.Double),
        ColumnDef("DIST_no", ColumnType.Double)
      )
      === categoricalModelSQLOutputFeatures(DIST, Seq("yes", "no"))
    )
    assert(
      Seq(
        ColumnDef("PRED", ColumnType.String),
        ColumnDef("DIST_yes_", ColumnType.Double),
        ColumnDef("DIST__no", ColumnType.Double)
      )
      === categoricalModelSQLOutputFeatures(DIST, Seq("yes*", "+no"))
    )
    assert(
      Seq(
        ColumnDef("PRED", ColumnType.String),
        ColumnDef("DIST_yes_no", ColumnType.Double),
        ColumnDef("DIST_yes_no_1", ColumnType.Double)
      )
      === categoricalModelSQLOutputFeatures(DIST, Seq("yes no", "yes_no"))
    )
  }

}

/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.util

import com.alpine.plugin.core.io.{ColumnDef, ColumnType}
import org.apache.commons.lang3.StringUtils

/**
 * This class is a utility for defining features, in particular the output features of models.
 */
object FeatureUtil {

  val PRED = "PRED"
  val CONF = "CONF"
  val DIST = "DIST"
  val INFO = "INFO"

  val simpleModelOutputFeatures: Seq[ColumnDef] = {
    Seq(new ColumnDef(PRED, ColumnType.String))
  }

  val regressionOutputFeatures: Seq[ColumnDef] = {
    Seq(new ColumnDef(PRED, ColumnType.Double))
  }

  val classificationOutputFeatures: Seq[ColumnDef] = {
    Seq(
      new ColumnDef(PRED, ColumnType.String),
      new ColumnDef(CONF, ColumnType.Double),
      new ColumnDef(INFO, ColumnType.Sparse)
    )
  }

  val clusteringOutputFeatures: Seq[ColumnDef] = {
    Seq(
      new ColumnDef(PRED, ColumnType.String),
      new ColumnDef(DIST, ColumnType.Double),
      new ColumnDef(INFO, ColumnType.Sparse)
    )
  }

  /**
    * Cleans up a string to make it suitable as a column name.
    * These names are used in Pig / SQL scripts, so they need to satisfy some constraints.
    *
    * Cleaning includes:
    * Strips accents;
    * replacing non-alphanumeric characters with "_";
    * prefixing with an "_" if the string starts with a digit;
    * truncating characters beyond the maximum length (optional).
    * @param s String to be sanitized.
    * @param maxLength Optional, length to truncate the string at.
    * @return Processed string suitable for use as a column name.
    */
  def sanitizeStringForColumnName(s: String, maxLength: Option[Int] = None): String = {
    val cleaned = StringUtils.stripAccents(s).replaceAll("[^\\w]", "_")
    val prefixed = {
      if (cleaned.matches("^\\d")) {
        "_" + cleaned
      } else {
        cleaned
      }
    }
    maxLength match {
      case Some(n) =>
        if (n >= prefixed.length) {
          prefixed
        } else {
          prefixed.substring(0, n)
        }
      case None => prefixed
    }
  }

  /**
    * Adds integer suffices to column names, to makes sure none are duplicated in the list.
    * The returned Seq of will be same length and have column types in the same order as the original,
    * but column names may be changed.
    * @param columns Seq of column definitions, which may contain duplicate names.
    * @return New Seq of column definitions with non-duplicate names.
    */
  def deDuplicate(columns: Seq[ColumnDef]): Seq[ColumnDef] = {
    columns.foldLeft(List[ColumnDef]())(
      (columnsSoFar: List[ColumnDef], column: ColumnDef) => {
        if (columnsSoFar.map(_.columnName).contains(column.columnName)) {
          var suffix = 1
          // Must check for name, not the whole column def.
          while (columnsSoFar.map(_.columnName).contains(column.columnName + "_" + suffix)) {
            suffix += 1
          }
          new ColumnDef(column.columnName + "_" + suffix, column.columnType)
        } else {
          column
        }
      } :: columnsSoFar
    ).reverse
  }

  def categoricalModelSQLOutputFeatures(detailPrefix: String, classLabels: Seq[String]): Seq[ColumnDef] = {
    val initialList = ColumnDef(FeatureUtil.PRED, ColumnType.String) ::
      classLabels.map(l => {
        val unprocessedName = detailPrefix + '_' + l
        val sanitizedName = sanitizeStringForColumnName(unprocessedName, Some(20))
        ColumnDef(sanitizedName, ColumnType.Double)
      }).toList
    // Sanitization may have resulted in duplicates.
    deDuplicate(initialList)
  }

}

/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

import com.alpine.plugin.core.io.ColumnDef

/**
  * A visual model that corresponds to a table in the results console.
  */
case class TabularVisualModel(content: Seq[Seq[String]], columnDefs: Seq[ColumnDef], messages: Seq[String])
  extends VisualModel

object TabularVisualModel {

  def apply(content: Seq[Seq[String]], columnDefs: Seq[ColumnDef]): TabularVisualModel = {
    new TabularVisualModel(content, columnDefs, Seq())
  }

  def apply(content: Seq[Seq[String]], columnDefs: Seq[ColumnDef], message: String): TabularVisualModel = {
    new TabularVisualModel(content, columnDefs, Seq(message))
  }
}

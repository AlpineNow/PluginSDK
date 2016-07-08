/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

import com.alpine.plugin.core.io.ColumnDef

/**
  * A visual model that corresponds to a table in the results console.
  */
case class TabularVisualModel(content: Seq[Seq[String]], columnDefs: Seq[ColumnDef]) extends VisualModel

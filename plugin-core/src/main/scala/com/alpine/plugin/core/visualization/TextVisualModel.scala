/*
 * COPYRIGHT (C) 2016 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

/**
  * A simple text content visualization.
  *
  * @param content the text to display.
  */
case class TextVisualModel(content: String) extends VisualModel

/**
  * Create a visualization of an HTML text element.
  * Use this rather than the [[TextVisualModel]] method if you want to include
  * HTML formatting elements (like a table, or bold text) in your visualization.
  *
  * @param html An HTML String
  */
case class HtmlVisualModel(html: String) extends VisualModel

case class JavascriptVisualModel(functionName: String, data: Option[Any]) extends VisualModel

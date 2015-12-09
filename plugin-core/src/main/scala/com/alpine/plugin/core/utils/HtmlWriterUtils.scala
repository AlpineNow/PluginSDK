package com.alpine.plugin.core.utils

object HtmlTabulator {

  /**
   * Formats a nested sequence of Strings as an HTML table.
   * Uses right alignment and 10 pixel padding to style the cells.
   */
  def format(table: Seq[Seq[String]]) = table match {
    case Seq() => ""
    case _ =>
      new HtmlTable(table).toString
  }

  /**
   * Allows the user to inject her own style tag into the html table's td elements.
   * The table will be built with <td style = \" + styleTag + \" > " for each of the cells.
   * @param table a sequence of rows in the table, where each row is a sequence of strings.
   * @param styleTag an css style element. The default to string method uses "padding-right:10px;"
   * @return the html string representation of the table
   */
  def format(table: Seq[Seq[String]], styleTag: String) = table match {
    case Seq() => ""
    case _ =>
      new HtmlTable(table, styleTag).toString
  }
}

/**
 * Taken from https://github.com/duncanmak/scala-for-the-impatient/blob/master/src/main/scala/ch11/Table.scala
 * Modified to create tables with padding
 * Allows the user to inject her own style tag into the html table's td elements.
 * The table will be built with <td style = \" + styleTag + \" > " for each of the cells.
 * @param styleTag an css style element. The default is "padding-right:10px;"
 * @param rows a sequence of sequences of strings, representing the table. Each row should be the
 *             same length
 */
class HtmlTable(rows: Seq[Seq[Any]], styleTag: String = "padding-right:10px;") {

  override def toString =
    "<table >" + rows.map(tr => "<tr>" +
      tr.map(td => "<td style = \"" + styleTag + "\" >" + td + "</td>").mkString + "</tr>").mkString +
      "</table>"
}

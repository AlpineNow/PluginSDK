/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.visualization

import java.util.Locale

import com.alpine.plugin.core.io.{HdfsFile, TabularDataset}

/**
  * Created by Jennifer Thompson on 2/16/17.
  */
trait HDFSVisualModelHelper {

  /**
    * Create a visualization for an Hdfs tabular dataset.
    * @param dataset A Hdfs tabular dataset that we want to visualize.
    * @return A visualization of the sample.
    */
  def createTabularDatasetVisualization(dataset: TabularDataset): TabularVisualModel

  /**
    * Create a visualization for an Hdfs tabular dataset.
    *
    * @param dataset     A Hdfs tabular dataset that we want to visualize.
    * @param rowsToFetch Number of rows to fetch.
    * @return A visualization of the sample.
    */
  def createTabularDatasetVisualization(dataset: TabularDataset, rowsToFetch: Int): TabularVisualModel

  /**
    * Gets the first few lines of the HdfsFile as plain text.
    * @param hdfsFile The file to preview.
    * @return The first few lines of the file as a visual model.
    */
  def createPlainTextVisualModel(hdfsFile: HdfsFile): TextVisualModel

  def locale: Locale

}

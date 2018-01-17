/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.io

/**
  * Created by Jennifer Thompson on 9/13/17.
  */
trait IODataFrame extends IOBase {

  // extend TabularDataset, so we can use partial schema with this output type?
  def tabularSchema: TabularSchema

}

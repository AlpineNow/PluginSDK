/*
 * COPYRIGHT (C) 2017 Alpine Data Labs Inc. All Rights Reserved.
*/
package com.alpine.plugin.core.io

/**
  * This can be passed from parent to child operator at design-time.
  * It is part of the input and output objects of OperatorGUINode#getOperatorStatus.
  * It is used to contain metadata about the IOBase objects that will be passed at runtime.
  *
  * e.g.
  * For a dataset: what the TabularSchema will be, what data-source it will be stored on.
  * For a RowModel: what the input and output features will be, what the string identifier will be.
  */
trait IOMetadata {

}

/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.visualization

/**
  * A visual model is a representation of the visual output of an operator.
  *
  * Developers should not implement this themselves, but use the ready made classes,
  * e.g. [[TabularVisualModel]], [[TextVisualModel]], [[HtmlVisualModel]], [[CompositeVisualModel]]
  * or those returned by [[VisualModelFactory]].
  */
trait VisualModel

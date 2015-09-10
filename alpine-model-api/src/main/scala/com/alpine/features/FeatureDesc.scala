/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.features

case class FeatureDesc[T](name: String, dataType: DataType[_ <: T])


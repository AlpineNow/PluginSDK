package com.alpine.plugin.core.dialog

/**
  * The advanced spark settings window
  */
trait AdvancedSparkSettingsBox extends DialogElement

case class SparkParameter(name: String,
                          displayName: String,
                          value: String,
                          userSpecified: Boolean,
                          overridden: Boolean)
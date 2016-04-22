package com.alpine.plugin.core.dialog

/**
  * The advanced spark settings window
  */
//toDO: Change to config rather than setting
trait AdvancedSparkSettingsBox extends DialogElement {

  def setAvailableValues(options : Iterator[String]): Unit

  def setAvailableValues(options : Map[String, String]): Unit

  def setParameters(options : Iterator[SparkParameter]): Unit

  def addSetting(name: String, value: String): Unit

  def getSetting(settingId: String): String
}

case class SparkParameter(name: String,
                          displayName: String,
                          value: String,
                          userSpecified: Boolean,
                          overridden: Boolean)
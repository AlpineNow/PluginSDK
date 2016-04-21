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

class SparkParameter(n: String, dn: String, v: String, us: Boolean, o: Boolean) {
  val name: String = n
  val displayName: String = dn
  val value: String = v
  val userSpecified: Boolean = us
  val overridden: Boolean = o
}
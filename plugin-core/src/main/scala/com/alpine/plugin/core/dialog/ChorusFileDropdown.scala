package com.alpine.plugin.core.dialog

/**
  * Created by rachelwarren on 11/18/16.
  */

trait ChorusFileDropdown extends DialogElement

case class ChorusFile(id: String, name: String) {
  val extension: String = {
    val splits = name.split('.')
    if (splits.length < 2) {
      // Chorus references Workflows just by name, without the .afm extension.
      ".afm"
    } else {
      "." + splits.last
    }
  }
}
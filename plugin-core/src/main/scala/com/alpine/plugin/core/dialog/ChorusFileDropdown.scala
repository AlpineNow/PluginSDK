package com.alpine.plugin.core.dialog

/**
  * Created by rachelwarren on 11/18/16.
  */
trait ChorusFileDropdown extends DialogElement {
  def getExtensionFilter: Seq[String]
}

case class ChorusFile(fileName: String, fileId: String) {
  val extension: String = {
    val splits = fileName.split('.')
    if (splits.length < 2) {
      // Chorus references Workflows just by name, without the .afm extension.
      ".afm"
    } else {
      "." + splits.last
    }
  }
}

case class PythonNotebook(chorusFile: ChorusFile,
                          userModifiedAt: String,
                          serverStatus: String)

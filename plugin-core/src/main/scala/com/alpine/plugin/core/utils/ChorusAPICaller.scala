package com.alpine.plugin.core.utils

import java.io.{File, InputStream}

import com.alpine.plugin.core.dialog.{ChorusFile, PythonNotebook}

import scala.util.Try

/**
  * Created by rachelwarren on 12/7/16.
  */
abstract class ChorusAPICaller {

  /**
    * If the workfile can be downloaded, returns an input stream containing the text of the file.
    * Warning, afm files cannot be downloaded with the chorus api.
    *
    * @param workFileId - Id of the workfile to retrieve
    * @return Try(input stream of text of workfile)
    */
  def getWorkfileAsInputStream(workFileId: String): Try[InputStream]

  /**
    * Runs a workfile and returns the workfile object if successful
    * Note: this will not fail if the workfile exists but cannot be run (e.g. if the notebook server
    * is down, the query may appear successful).
    * Hoping to change this behavior in future releases.
    */
  def runWorkfile(workfileId: String): Try[ChorusFile]

  def runNotebook(workfileId: String): Try[PythonNotebook]

  def createOrUpdateChorusFile(workspaceId: String, file: File, overwrite: Boolean): Try[ChorusFile]

}

package com.alpine.plugin.core.utils

import java.io._

import com.alpine.plugin.core.ExecutionContext
import com.alpine.plugin.core.dialog.ChorusFile

import scala.util.Try

/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

/**
  * Created by emiliedelongueau on 2/14/17.
  */
object ChorusUtils {

  /** A method to write a (formatted) text to a Chorus workFile in the current workspace.
    *
    * @param chorusFileName     the final name of the Chorus file to create (including the extension)
    * @param text               text to write (html-formatted, etc...)
    * @param chorusAPICaller    created from getChorusAPICaller in OperatorParameters class
    * @param context            the Execution Context
    * @param newVersionIfExists if set to true and the chorusFileName already exists, a new version will be created
    * @return either Success[ChorusFile] or a Failure with the error message
    */
  def writeTextChorusFile(
                           chorusFileName: String,
                           text: String,
                           chorusAPICaller: ChorusAPICaller,
                           context: ExecutionContext,
                           newVersionIfExists: Boolean): Try[ChorusFile] = {

    val workspaceID = context.workflowInfo.workflowID

    //A temporary file is created, which we will then try to push to Chorus workspace.
    val tmpDir = context.recommendedTempDir
    val outputFile: File = new File(tmpDir, chorusFileName)

    //delete file if exists to prevent name collisions
    if (outputFile.exists()) {
      outputFile.delete()
    }

    val fw = new FileWriter(outputFile)
    val bw = new BufferedWriter(fw)
    try {
      bw.write(text)
    }
    finally {
      bw.close()
      fw.close()
    }

    val chorusFile = chorusAPICaller.createOrUpdateChorusFile(workspaceID, outputFile, newVersionIfExists)
    outputFile.delete()
    chorusFile
  }
}
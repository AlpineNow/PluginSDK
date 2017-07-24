package com.alpine.plugin.core.utils

import java.io.{File, InputStream}

import com.alpine.plugin.core._
import com.alpine.plugin.core.dialog.ChorusFile

import scala.collection.immutable
import scala.util.Try

/**
  * Created by rachelwarren on 12/7/16.
  */
abstract class ChorusAPICaller {


  def getNotebookDetails(notebookID: String): Try[NotebookDetails]

  /**
    * Gets the workfile from Chorus as an input stream, applies the read function,
    * closes the input stream and then returns the result of the read function.
    *
    * @param workFileID   Id of the file to download.
    * @param readFunction The function to apply to the file input stream.
    * @tparam R The return type of the read function.
    * @return The output of the readFunction applied to the downloaded file input stream.
    */
  def readWorkfileInputStream[R](workFileID: String, readFunction: InputStream => R): Try[R]

  /**
    * Equivalent to [[readWorkfileInputStream(workfile.id, readFunction)]]
    *
    * @param workfile     The file to download.
    * @param readFunction The function to apply to the file input stream.
    * @tparam R The return type of the read function.
    * @return The output of the readFunction applied to the downloaded file input stream.
    */
  def readWorkfileInputStream[R](workfile: ChorusFile, readFunction: InputStream => R): Try[R]

  /**
    * Downloads the workfile with the referenced id from Chorus,
    * and reads the content as a text file.
    *
    * @param workFileID Id of the file to download.
    * @return The contents of the file as a string.
    */
  def readWorkfileAsText(workFileID: String): Try[String]

  /**
    * Equivalent to [[readWorkfileAsText(workfile.id)]]
    *
    * @param workfile The file to download.
    * @return The contents of the file as a string.
    */
  def readWorkfileAsText(workfile: ChorusFile): Try[String]

  /**
    * Runs a workfile and returns the workfile object if successful
    * Note: this will not fail if the workfile exists but cannot be run (e.g. if the notebook server
    * is down, the query may appear successful).
    * Hoping to change this behavior in future releases.
    */
  def runWorkfile(workfileId: String): Try[ChorusFile]

  def runNotebook(workfileId: String): Try[PythonNotebook]

  /**
    * Runs a notebook by substituting the output path (and possibly input path(s)).
    * This will only work if the notebook attribute "ready_to_execute" is true.
    *
    * @param notebook_id        Id of the notebook to retrieve
    * @param dataSourceName     name of the data source where inputs come from and where output will be stored
    * @param notebookInputs     Optional Seq[[NotebookIOForExecution]] of inputs info for substitution in the notebook
    * @param notebookOutput     Optional [[NotebookIOForExecution]]  or single output substitution in the notebook
    * @param sparkParametersMap Optional Spark parameters to be substituted in notebook if Spark is used (e.g Map("spark.executor.instances" -> "2")
    * @return
    */
  def runNotebookExecution(
    notebook_id: String,
    dataSourceName: String,
    notebookInputs: Option[Seq[NotebookIOForExecution]],
    notebookOutput: Option[NotebookIOForExecution],
    sparkParametersMap: Option[immutable.Map[String, String]]): Try[PythonNotebookExecution]

  /**
    * Queries the status of the notebook execution.
    *
    * @param notebook_id  Id of the notebook
    * @param execution_id Id of the execution to retrieve from PythonNotebookExecution response.
    */
  def getNotebookExecutionStatus(notebook_id: String, execution_id: String): Try[PythonNotebookExecution]

  def createOrUpdateChorusFile(currentWorkflowID: String, file: File, overwrite: Boolean): Try[ChorusFile]

  /**
    * Lists the DB connection names (as registered in Chorus 'Data' tab) and ids for a specific workflow
    *
    * @param workfileID id of the current workflow
    * @return The list of Chorus DB data sources [[ChorusDBSourceInfo]] the workflow is connected to.
    */
  def getWorkfileDBDataSources(workfileID: String): Try[List[ChorusDBSourceInfo]]

}

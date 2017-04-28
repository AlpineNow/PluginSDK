package com.alpine.plugin.test.mock

import java.io.{File, FileInputStream, InputStream}

import com.alpine.plugin.core.dialog.ChorusFile
import com.alpine.plugin.core.utils.ChorusAPICaller
import com.alpine.plugin.core.{NotebookDetails, PythonNotebook, PythonNotebookExecution}

import scala.util.Try

class ChorusAPICallerMock(val workfiles: Seq[ChorusFileInWorkspaceMock]) extends ChorusAPICaller {

  val workfileMap: Map[String, ChorusFileInWorkspaceMock] = {
    workfiles.map(w => (w.wf.id, w)).toMap
  }

  override def readWorkfileInputStream[R](workFileID: String, readFunction: (InputStream) => R): Try[R] = {
    Try {
      val mockWf = this.workfileMap(workFileID)
      val workfilePath = mockWf.workfilePath.get
      val f = new File(workfilePath)
      val stream = new FileInputStream(f)
      try {
        readFunction(stream)
      } finally {
        stream.close()
      }
    }
  }

  override def readWorkfileInputStream[R](workfile: ChorusFile, readFunction: (InputStream) => R): Try[R] = {
    readWorkfileInputStream(workfile.id, readFunction)
  }

  override def readWorkfileAsText(workFileID: String): Try[String] = {
    val readFunction: InputStream => String = {
      in: InputStream => scala.io.Source.fromInputStream(in).getLines().mkString("\n")
    }
    readWorkfileInputStream(workFileID, readFunction)
  }

  override def readWorkfileAsText(workfile: ChorusFile): Try[String] = {
    readWorkfileAsText(workfile.id)
  }
  /**
    * Runs a workfile and returns the workfile object if successful
    * Note: this will not fail if the workfile exists but cannot be run (e.g. if the notebook server
    * is down, the query may appear successful).
    * Hoping to change this behavior in future releases.
    */
  override def runWorkfile(workfileId: String): Try[ChorusFile] = {
    Try(this.workfileMap(workfileId).wf)
  }

  override def runNotebook(workfileId: String): Try[PythonNotebook] = ???

  override def getNotebookDetails(notebookID: String): Try[NotebookDetails] = ???

  override def runNotebookExecution(notebook_id: String, input_path1: String, outputPath: String): Try[PythonNotebookExecution] = ???

  override def getNotebookExecutionStatus(notebook_id: String, execution_id: String): Try[PythonNotebookExecution] = ???

  override def createOrUpdateChorusFile(workspaceId: String, file: File, overwrite: Boolean): Try[ChorusFile] = ???

}

object ChorusAPICallerMock {
  def apply(): ChorusAPICallerMock = {
    val emptyWorkfiles = Seq[ChorusFileInWorkspaceMock]()
    new ChorusAPICallerMock(emptyWorkfiles)
  }

}

case class ChorusFileInWorkspaceMock(wf: ChorusFile, workfilePath: Option[String])

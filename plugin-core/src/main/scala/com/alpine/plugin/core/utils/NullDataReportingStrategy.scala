package com.alpine.plugin.core.utils



sealed trait NullDataReportingStrategy{
}

case class DoNotCount() extends NullDataReportingStrategy{}

case class DoNotWrite() extends NullDataReportingStrategy{}

case class WriteAndCount(path : String,
                         storageFormatType: HdfsStorageFormatType,
                         overwrite : Boolean) extends NullDataReportingStrategy{}

//TODO: I really need this?
object NullDataReportingStrategy {
  val DoNotCount = new DoNotCount()
  val DoNotWrite = new DoNotWrite()
  def WriteAndCount(path : String): NullDataReportingStrategy = new  WriteAndCount(path,
    HdfsStorageFormatType.CSV, true)

  val noCountDisplay = "Do Not Write or Count Null Rows (Fastest)"
  val doNotWriteDisplay = "Do Not Write Null Rows to File"
  val writeAndCountDisplay =  "Write All Null Rows to File"

  val displayOptions = Seq(noCountDisplay, doNotWriteDisplay,
    writeAndCountDisplay)


}
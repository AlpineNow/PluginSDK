package com.alpine.plugin.test

import java.io.{File, FileOutputStream, IOException}
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.spark.Logging

import scala.collection.JavaConversions._

class MiniCluster(dataNodes: Int = 1, taskTrackers: Int = 1) extends Logging {

  private val configurationFilePath = new File(this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile.getAbsolutePath + "/hadoop-site.xml"

  var miniDFSCluster: MiniDFSCluster = null
  var miniYarnCluster: MiniYARNCluster = null
  var configuration: Configuration = null
  var fileSystem: FileSystem = null
  var tempDir: File = _

  def createDirectory(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir
  }
  
  private val LOG4J_CONF = """
                             |log4j.rootCategory=DEBUG, console
                             |log4j.appender.console=org.apache.log4j.ConsoleAppender
                             |log4j.appender.console.target=System.err
                             |log4j.appender.console.layout=org.apache.log4j.PatternLayout
                             |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
                           """.stripMargin

  def start() {
    // most is copypast from spark.YarnClusterSuite
    val conf = new HdfsConfiguration(new YarnConfiguration())
    tempDir = createDirectory()
    val logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()
    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, Charsets.UTF_8)
    val childClasspath = logConfDir.getAbsolutePath() + File.pathSeparator +
      sys.props("java.class.path")

    val oldConf = sys.props.filter { case (k, v) => k.startsWith("spark.") }.toMap

    miniYarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1)
    miniYarnCluster.init(conf)
    miniYarnCluster.start()

    // There's a race in MiniYARNCluster in which start() may return before the RM has updated
    // its address in the configuration. You can see this in the logs by noticing that when
    // MiniYARNCluster prints the address, it still has port "0" assigned, although later the
    // test works sometimes:
    //
    //    INFO MiniYARNCluster: MiniYARN ResourceManager address: blah:0
    //
    // That log message prints the contents of the RM_ADDRESS config variable. If you check it
    // later on, it looks something like this:
    //
    //    INFO YarnClusterSuite: RM address in configuration is blah:42631
    //
    // This hack loops for a bit waiting for the port to change, and fails the test if it hasn't
    // done so in a timely manner (defined to be 10 seconds).
    val config = miniYarnCluster.getConfig()
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      logDebug("RM address still not set in configuration, waiting...")
      TimeUnit.MILLISECONDS.sleep(100)
    }
    logInfo(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")
    config.foreach { e =>
      sys.props += ("spark.hadoop." + e.getKey() -> e.getValue())
    }

    val fakeSparkJar = File.createTempFile("sparkJar", null, tempDir)
    sys.props += ("spark.yarn.jar" -> ("local:" + fakeSparkJar.getAbsolutePath()))
    sys.props += ("spark.executor.instances" -> "1")
    sys.props += ("spark.driver.extraClassPath" -> childClasspath)
    sys.props += ("spark.executor.extraClassPath" -> childClasspath)

    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }

    miniDFSCluster = new MiniDFSCluster.Builder(conf).nameNodePort(9020).format(true).build()
    miniDFSCluster.waitClusterUp()

    val r = miniDFSCluster.getConfiguration(0)
    miniDFSCluster.getFileSystem.mkdir(new Path("/tmp"), new FsPermission(777.toShort))

    configuration = miniYarnCluster.getConfig
    configuration.writeXml(new FileOutputStream(configurationFile))
    fileSystem = miniDFSCluster.getFileSystem
  }

  def shutdown() {
    miniDFSCluster.getFileSystem.close()
    miniDFSCluster.shutdown()
    miniYarnCluster.stop()
    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }
  }
}
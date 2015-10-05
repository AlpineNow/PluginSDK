/**
 * ASF licensed, based on SQLContext object & SparkContext object in Spark 1.4
 * once we upgrade replace with regular SQLContext and SparkContext
 */
package com.alpine.plugin.core.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean, AtomicInteger}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
object SparkContextSingleton {

  /**
   * Lock that guards access to global variables that track SparkContext construction.
   */
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
   */
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  /**
   * Points to a partially-constructed SparkContext if some thread is in the SparkContext
   * constructor, or `None` if no SparkContext is being constructed.
   *
   * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
   */
  private var contextBeingConstructed: Option[SparkContext] = None

  /**
   * Called to ensure that no other SparkContext is running in this JVM.
   *
   * Throws an exception if a running context is detected and logs a warning if another thread is
   * constructing a SparkContext.  This warning is necessary because the current locking scheme
   * prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private def assertNoOtherContextIsRunning(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      contextBeingConstructed.foreach { otherContext =>
        if (activeContext.get() != null) {
          val ctx = activeContext.get()
          val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
            " To ignore this error, set spark.driver.allowMultipleContexts = true. "
          val exception = new Exception(errMsg)
          if (!allowMultipleContexts) {
            throw exception
          }
        }
      }
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * Note: This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      }
      activeContext.get()
    }
  }

  /**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * This method allows not passing a SparkConf (useful if just retrieving).
   *
   * Note: This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   */
  def getOrCreate(): SparkContext = {
    getOrCreate(new SparkConf().setMaster("local").setAppName("pandamagic-aka-local-dataframe-discovery"))
  }

  /**
   * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
   * running.  Throws an exception if a running context is detected and logs a warning if another
   * thread is constructing a SparkContext.  This warning is necessary because the current locking
   * scheme prevents us from reliably distinguishing between cases where another context is being
   * constructed and cases where another constructor threw an exception.
   */
  private[spark] def markPartiallyConstructed(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = Some(sc)
    }
  }

  /**
   * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
   * raced with this constructor and started.
   */
  private[spark] def setActiveContext(
      sc: SparkContext,
      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc, allowMultipleContexts)
      contextBeingConstructed = None
      activeContext.set(sc)
    }
  }

  /**
   * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
   * also called in unit tests to prevent a flood of warnings from test suites that don't /
can't
   * properly clean up their SparkContexts.
   */
  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }
}

/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the last created SQLContext instance.
 */
object SQLContextSingleton {

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SQLContext]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        lastInstantiatedContext.set(new SQLContext(sparkContext))
      }
    }
    lastInstantiatedContext.get()
  }
}

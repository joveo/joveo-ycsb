package com.joveox.ycsb.common

import java.nio.file.Paths
import java.util.Properties

import com.yahoo.ycsb.{DB, Workload}
import org.apache.logging.log4j.scala.Logging


class JoveoYCSBWorkload extends Workload with Logging{

  protected var isLoad = false
  protected var configManager: ConfigManager = _

  override def init(p: Properties): Unit = {
    super.init(p)
    isLoad = ! p.getProperty( "dotransactions", isLoad.toString ).toBoolean
    val confPath = Paths.get( p.getProperty("joveo.ycsb.conf") )
    configManager = ConfigManager.init( confPath, isLoad )
  }

  override def initThread(p: Properties, threadId: Int, totalThreads: Int): AnyRef = {
    super.initThread( p, threadId, totalThreads )
    configManager.operationManager.iterator( threadId, totalThreads )
  }

  override def doInsert( db: DB, threadState: Any ): Boolean = {
    runNext( db, threadState )
  }

  override def doTransaction(db: DB, threadState: Any): Boolean = {
    runNext( db, threadState )
  }

  def runNext( db: DB, threadState: Any ): Boolean = {
    val iterator = threadState.asInstanceOf[ OperationIterator ]
    if( iterator.hasNext ) {
      val op = iterator.next()
      val status = op.runNext( db, iterator.threadId, iterator.idx )
      status != null && status.isOk
    }
    else false
  }

  override def cleanup(): Unit = {
    super.cleanup()
  }

}

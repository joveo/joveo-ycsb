package com.joveox.ycsb.common

import java.nio.file.Paths
import java.util.Properties

import com.yahoo.ycsb.{DB, Workload}
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable


class JoveoYCSBWorkload extends Workload with Logging{

  protected var isLoad = false
  protected var configManager: ConfigManager = _

  override def init(p: Properties): Unit = {
    super.init(p)
    isLoad = ! p.getProperty( "dotransactions", isLoad.toString ).toBoolean
    val confPath = Paths.get( p.getProperty("joveo.ycsb.conf") )
    configManager = ConfigManager.init( confPath, isLoad )
  }

  private val generators = mutable.HashMap.empty[ Int,  UseCaseGenerator ]

  override def initThread(p: Properties, threadId: Int, totalThreads: Int): AnyRef = {
    super.initThread( p, threadId, totalThreads )
    val generator = configManager.useCaseGenerator( threadId, totalThreads )
    generators.update( threadId, generator )
    generator
  }

  override def doInsert( db: DB, threadState: Any ): Boolean = {
    runNext( db, threadState )
  }

  override def doTransaction(db: DB, threadState: Any): Boolean = {
    runNext( db, threadState )
  }

  def runNext( db: DB, threadState: Any ): Boolean = {
    val iterator = threadState.asInstanceOf[ UseCaseGenerator ]
    if( iterator.hasNext ) {
      val op = iterator.next()
      op.runNext( db, configManager.schema, iterator.threadId, iterator.idx )
    }
    true
  }

  override def cleanup(): Unit = {
    super.cleanup()
    generators.values.foreach( _.cleanup() )
  }

}


package com.joveox.ycsb.common

import java.util

import com.yahoo.ycsb.measurements.Measurements
import com.yahoo.ycsb.{ByteIterator, DB, Status}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait DBExtension{
  type Entity = ( String, util.Map[ String, ByteIterator ] )
  def init( config: Map[ String, String ], schema: Schema, global: Any ): Unit
  def initGlobal( config: Map[ String, String ], schema: Schema, useCaseStore: UseCaseStore ): Any
  def getKey( key: String ): String = ""
  def read( op: Read )( key: String ): Status
  def bulkRead( op: Read )( keys: List[ String ] ): Status
  def update( op: Update )( entity: Entity ): Status
  def bulkUpdate( op: Update )( entities: List[ Entity ] ): Status
  def insert( op: Create )( entity: Entity ): Status
  def bulkInsert( op: Create )( entities: List[ Entity ] ): Status
      //TODO: Add delete and scan
  def cleanup(): Unit
  def cleanupGlobal(): Unit

}

object JoveoDB {
  private var isGlobalInitDOne = false
  private var cached: Any = _
  private var isCleanupInitDone = false
  def globalInit( db: DBExtension, config: Map[ String, String ], schema: Schema, useCaseStore: UseCaseStore  ): Any = {
    synchronized{
      if( !isGlobalInitDOne )
        cached = db.initGlobal( config, schema, useCaseStore )
      isGlobalInitDOne = true
    }
    cached
  }
  def globalCleanup( db : DBExtension): Unit = {
    synchronized{
      if( !isCleanupInitDone )
        db.cleanupGlobal()
      isCleanupInitDone = true
    }
  }
}

class JoveoDB extends DB with Logging{

  type Entity = ( String, util.Map[ String, ByteIterator ] )

  protected val measurements = Measurements.getMeasurements

  protected var readQueues: Map[ UseCase, Batched[ String ] ] = _
  protected var writeQueues: Map[ UseCase, Batched[ Entity ] ] = _

  protected var useCaseStore: UseCaseStore = _

  protected var inner: DBExtension = _

  override def init(): Unit = {
    super.init()
    val conf = ConfigManager.get.dbConf
    val schema = ConfigManager.get.schema
    useCaseStore = ConfigManager.get.useCaseStore
    inner = getClass.getClassLoader.loadClass( conf.dbClass ).getDeclaredConstructor().newInstance().asInstanceOf[ DBExtension ]
    val global = JoveoDB.globalInit( inner, conf.config, schema, useCaseStore )
    inner.init( conf.config, schema, global )
    readQueues = useCaseStore.reads.map{ useCase =>
      useCase -> new Batched[ String ]( useCase.batchSize )
    }.toMap
    writeQueues = ( useCaseStore.updates ++  useCaseStore.inserts ).map{ useCase =>
      useCase -> new Batched[ Entity ]( useCase.batchSize )
    }.toMap
  }

  override def cleanup(): Unit = {
    super.cleanup()
    inner.cleanup()
    JoveoDB.globalCleanup( inner )
  }


  protected def getRead( fields: util.Set[ String ] ): Read = {
    useCaseStore.read( fields ) match {
      case None => throw new IllegalStateException(s"JoveoDB: No Read operation found for ${fields.asScala.mkString(",")} ")
      case Some( useCase ) => useCase
    }
  }

  protected def getUpdate( fields: util.Set[ String ] ): Update = {
    useCaseStore.update( fields ) match {
      case None => throw new IllegalStateException(s"JoveoDB: No Update operation found for ${fields.asScala.mkString(",")} ")
      case Some( useCase ) => useCase
    }
  }

  protected def getInsert( fields: util.Set[ String ] ): Create = {
    useCaseStore.insert( fields ) match {
      case None => throw new IllegalStateException(s"JoveoDB: No Create operation found for ${fields.asScala.mkString(",")} ")
      case Some( useCase ) => useCase
    }
  }

  protected def runSafeWithLogging( fn: () => Status ): Status = {
    Try {
      fn()
    } match {
      case Failure( ex ) =>
        logger.warn(" Error in JoveoDB ", ex )
        Status.ERROR
      case Success( status ) => status
    }
  }

  protected def exec[ Value ](
                                    key: String, value: Value,
                                    useCase: UseCase,
                                    run: Value => Status,
                                    batchRun: List[ Value ] => Status
                                  ): Status = {
    val intendedStartTime = measurements.getIntendedtartTimeNs
    val startTime = System.nanoTime()
    val status = if( useCase.batchSize == 1){
      runSafeWithLogging( () => run(value) )
    }
    else{
      val batchKey = inner.getKey( key )
      val batched = useCase match {
        case v: Read => readQueues( useCase )
        case v: Write => writeQueues( useCase )
      }
      batched.asInstanceOf[Batched[Value]].enqueue(batchKey, value) match {
        case Nil => Status.BATCHED_OK
        case values =>
          runSafeWithLogging( () => batchRun( values ) )
      }
    }
    val endTime = System.nanoTime()
    measure( useCase, status, intendedStartTime, startTime, endTime )
    report(useCase, status )
    status
  }

  override def read( table: String, key: String, fields: util.Set[String], result: util.Map[String, ByteIterator] ): Status = {
    val useCase = getRead( fields )
    exec( key, key, useCase,  inner.read( useCase ), inner.bulkRead( useCase )  )
  }

  override def update(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    val useCase = getUpdate( values.keySet() )
    exec( key, key -> values, useCase,  inner.update( useCase ), inner.bulkUpdate( useCase ) )
  }

  override def insert(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    val useCase = getInsert( values.keySet() )
    exec( key, key -> values, useCase,  inner.insert( useCase ), inner.bulkInsert( useCase ) )
  }


  def measure( useCase: UseCase, result: Status, intendedStartTimeNanos: Long, startTimeNanos: Long, endTimeNanos: Long): Unit = {
    val measurementName = useCase.name + ( if (result == null || !(result.isOk)) "-FAILED" else "" )
    measurements.measure(measurementName, ((endTimeNanos - startTimeNanos) / 1000).toInt)
    measurements.measureIntended(measurementName, ((endTimeNanos - intendedStartTimeNanos) / 1000).toInt)
  }

  def report( useCase: UseCase, result: Status): Unit = {
    measurements.reportStatus(useCase.name,  result )
  }


  override def delete(table: String, key: String): Status = ???
  override def scan(
                     table: String,
                     startkey: String,
                     recordcount: Int,
                     fields: util.Set[String],
                     result: util.Vector[util.HashMap[String, ByteIterator]]
                   ): Status = ???

}


class Batched[ Value ]( batchSize: Int ) {

  private val queues = mutable.HashMap.empty[ String, mutable.Queue[ Value ] ]

  def enqueue( key: String, value: Value ): List[ Value ] = {
    val queue = queues.getOrElseUpdate( key, mutable.Queue.empty[ Value ] )
    queue.enqueue( value )
    if( queue.size >= batchSize ) {
      queue.dequeueAll(_ => true).toList
    }
    else List.empty
  }

}
package com.joveox.ycsb.common

import java.util

import com.yahoo.ycsb.{ByteIterator, DB, Status}

import scala.collection.JavaConverters._
import scala.collection.mutable


abstract class JoveoDBBatch extends DB {
  type Entity = ( String, util.Map[ String, ByteIterator ] )
  type BatchKey

  protected var reads: Batched[ BatchKey, String, Status ] = _
  protected var updates: Batched[ BatchKey, Entity, Status ] = _
  protected var inserts: Batched[ BatchKey, Entity, Status ] = _

  protected var readBatchSize = 30
  protected var insertsBatchSize = 10
  protected var updateBatchSize = 10

  protected var operationManager: YCSBOperationManager = _

  override def init(): Unit = {
    super.init()
    val properties = getProperties
    readBatchSize = properties.getProperty( "db.batch.reads", readBatchSize.toString ).toInt
    insertsBatchSize = properties.getProperty( "db.batch.inserts", insertsBatchSize.toString ).toInt
    updateBatchSize = properties.getProperty( "db.batch.updates", updateBatchSize.toString ).toInt
    operationManager = YCSBOperationManager.get
  }

  protected def getKey( op: DBOperation, id: String, operation: YCSBOperation ): BatchKey
  protected def bulkRead( op: YCSBOperation )( ids: List[ String ] ): Status
  protected def bulkWrite( op: YCSBOperation )( entities: List[ Entity ] ): Status

  protected def exec[ Value ](
                               key: BatchKey,
                               batched: Batched[ BatchKey, Value, Status ],
                               batchSize: Int,
                               batchedRun: List[ Value ] => Status,
                               value: Value
                             ): ( Status, Batched[ BatchKey, Value, Status ] ) = {
    val queue = if( batched == null ) {
      new Batched[ BatchKey, Value, Status ]( batchSize )
    }
    else batched
    queue.enqueue( key, value, batchedRun ) match {
      case ( true, Some( status ) ) => status -> queue
      case ( false, None ) => Status.BATCHED_OK -> queue
      case v => throw new IllegalStateException(s" JoveoDBBatch: Batched enqueue returned unexpected result $v")
    }
  }

  protected def getOperation( op: DBOperation, fields: util.Set[ String ] ): YCSBOperation = {
    operationManager.get( op, fields ) match {
      case Nil => throw new IllegalStateException(s"JoveoDBBatch: No prepared statement found for ($op,${fields.asScala.mkString(",")}) ")
      case head :: Nil => head
      case values => throw new IllegalStateException(
        s"JoveoDBBatch: More than 1 prepared statement found. Values ${
          values.map(_.toString()).mkString("\n")
        }"
      )
    }
  }

  protected def write(
                       op: DBOperation,
                       table: String,
                       key: String,
                       values: util.Map[String, ByteIterator]
                     ): Status = {
    val ycsbOp = getOperation( op, values.keySet() )
    val queueKey = getKey( op, key, ycsbOp )
    val value: Entity = key -> values
    val ( status, queue ) = exec(
      queueKey,
      if( op == DBOperation.CREATE ) inserts else updates,
      if( op == DBOperation.CREATE ) insertsBatchSize else updateBatchSize,
      bulkWrite( ycsbOp ),
      value
    )
    if( op == DBOperation.CREATE )
      inserts = queue
    else updates = queue
    status
  }


  override def read(
                     table: String,
                     key: String,
                     fields: util.Set[String],
                     result: util.Map[String, ByteIterator]
                   ): Status = {
    val ycsbOp = getOperation( DBOperation.READ, fields )
    val queueKey = getKey( DBOperation.READ, key, ycsbOp )
    val ( status, queue ) = exec( queueKey, reads, readBatchSize, bulkRead( ycsbOp ), key )
    reads = queue
    status
  }

  override def update(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    write( DBOperation.UPDATE, table, key, values )
  }

  override def insert(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    write( DBOperation.CREATE, table, key, values )
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


class Batched[ Key, Value, Result ]( batchSize: Int ) {

  private val queues = mutable.HashMap.empty[ Key, mutable.Queue[ Value ] ]

  def enqueue( key: Key, value: Value, run: List[ Value ] => Result ): ( Boolean, Option[ Result] ) = {
    val queue = queues.getOrElseUpdate( key, mutable.Queue.empty[ Value ] )
    queue.enqueue( value )
    if( queue.size >= batchSize ) {
      val values = queue.dequeueAll(_ => true).toList
      val result = run( values )
      true -> Some( result )
    }
    else false -> None
  }

}
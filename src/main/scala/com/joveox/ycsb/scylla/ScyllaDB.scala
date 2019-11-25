package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, PreparedStatement, ResultSet}
import com.joveox.ycsb.common.DBOperation
import com.yahoo.ycsb.{ByteArrayByteIterator, ByteIterator, DB, Status}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class ScyllaDB extends DB with Logging {

  type PreparedStatementId = ByteBuffer
  type Hosts = Set[ UUID ]
  type BatchKey = ( PreparedStatement, Hosts )

  protected var reads: Batched[ BatchKey, String, Status ] = _
  protected var updates: Batched[ BatchKey, BoundStatement, Status ] = _
  protected var inserts: Batched[ BatchKey, BoundStatement, Status ] = _

  protected var readBatchSize = 30
  protected var insertsBatchSize = 10
  protected var updateBatchSize = 10

  protected var conf: ScyllaConf = _
  protected var session: CqlSession = _

  protected var preparedStatementManager: ScyllaPreparedStatementsManager = _

  override def init(): Unit = {
    super.init()
    val properties = getProperties
    conf = ScyllaConf( properties )
    session = ScyllaDBSession.build( conf )
    preparedStatementManager = ScyllaPreparedStatementsManager.get( properties )
    readBatchSize = properties.getProperty( "scylla.batch.reads", readBatchSize.toString ).toInt
    insertsBatchSize = properties.getProperty( "scylla.batch.inserts", insertsBatchSize.toString ).toInt
    updateBatchSize = properties.getProperty( "scylla.batch.updates", updateBatchSize.toString ).toInt
  }

  override def cleanup(): Unit = {
    session.close()
    preparedStatementManager.close()
  }


  protected def hosts( key: String ): Hosts = {
    val tokenMap = session.getMetadata.getTokenMap
    val routingKey = TypeCodecs.TEXT.encode( key, session.getContext.getProtocolVersion )
    if( ! tokenMap.isPresent )
      Set.empty[ UUID ]
    else
      tokenMap.get().getReplicas( conf.keyspace, routingKey).asScala.map(_.getHostId).toSet
  }

  protected def getKey( op: DBOperation, key: String, fields: util.Set[String] ): BatchKey = {
    val prepared = preparedStatementManager.get( op, fields )
    val nodes = hosts( key )
    prepared -> nodes
  }

  protected def bulkRead( prepared: PreparedStatement )( values: List[ String ] ): Status = {
    val result = new util.HashMap[String, ByteIterator ]()
    val stmt = preparedStatementManager.bindRead( values.toSet, prepared )
    readTo( result, () => session.execute( stmt ) )
  }

  protected def bulkWrite( values: List[ BoundStatement ] ): Status = {
    values match {
      case Nil => Status.ERROR
      case head :: Nil => persist( () => session.execute( head ) )
      case _ =>
        val stmt = BatchStatement.newInstance( DefaultBatchType.UNLOGGED )
          .addAll(
            values.asJava
          )
        persist( () => session.execute(stmt) )
    }
  }

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
      case v => throw new IllegalStateException(s" Batched enqueue returned unexpected result $v")
    }
  }

  override def read(
                     table: String,
                     key: String,
                     fields: util.Set[String],
                     result: util.Map[String, ByteIterator]
                   ): Status = {
    val queueKey = getKey( DBOperation.READ, key, fields )
    val ( status, queue ) = exec( queueKey, reads, readBatchSize, bulkRead( queueKey._1 ), key )
    reads = queue
    status
  }

  override def update(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    write( DBOperation.UPDATE, table, key, values )
  }

  override def insert(table: String, key: String, values: util.Map[String, ByteIterator]): Status = {
    write( DBOperation.CREATE, table, key, values )
  }

  protected def write(
                      op: DBOperation,
                      table: String,
                      key: String,
                      values: util.Map[String, ByteIterator]
                     ): Status = {
    val queueKey = getKey( op, key, values.keySet() )
    val bounded = preparedStatementManager.bindWrite( key, values, queueKey._1 )
    val ( status, queue ) = exec(
      queueKey,
      if( op == DBOperation.CREATE ) inserts else updates,
      if( op == DBOperation.CREATE ) insertsBatchSize else updateBatchSize,
      bulkWrite,
      bounded
    )
    if( op == DBOperation.CREATE )
      inserts = queue
    else updates = queue
    status
  }

  protected def persist( fn: () => ResultSet ): Status = {
    Try{
      fn()
    } match {
      case Success( result ) =>
        if( result.wasApplied() ) Status.OK else Status.ERROR
      case Failure( ex ) =>
        logger.error( " ERROR Scylla persist: ", ex)
        Status.ERROR
    }
  }

  protected def readTo(
                        values: util.Map[ String, ByteIterator ],
                        fn: () => ResultSet
                      ): Status = {
    Try{
      fn()
    } match {
      case Success( rs ) =>
        rs.all().asScala.zipWithIndex.foreach{
          case ( row, i) =>
            val columns = row.getColumnDefinitions
            columns.asScala.foreach{ col =>
              val content = row.getBytesUnsafe( col.getName.toString )
              if( content != null )
                values.put( s"$i--"+col.getName.toString, new ByteArrayByteIterator( content.array() ) )
              else
                values.put( s"$i--"+col.getName.toString, null  )
            }
        }
        Status.OK
      case Failure( ex ) =>
        logger.error( " ERROR Scylla readTo: ", ex)
        Status.ERROR
    }
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
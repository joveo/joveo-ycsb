package com.joveox.ycsb.scylla

import java.util
import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, ResultSet}
import com.joveox.ycsb.common.{Create, DBExtension, DBOperation, JVBlob, Read, Schema, Update, UseCase, UseCaseStore}
import com.yahoo.ycsb.{ByteIterator, Status}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ScyllaDB extends DBExtension with Logging {

  protected var keyspace: String = _
  protected var session: CqlSession = _
  protected var preparedStatementManager: ScyllaPreparedStatementsStore = _

  override def init( config: Map[ String, String ], schema: Schema, global: Any ): Unit = {
    keyspace = schema.db
    session = ScyllaSession build(config, true)
    preparedStatementManager = global.asInstanceOf[ ScyllaPreparedStatementsStore ]
  }

  override def cleanup(): Unit = {
    session.close()
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
                        expected: Int,
                        values: util.Map[ String, ByteIterator ],
                        fn: () => ResultSet
                      ): Status = {
    Try{
      fn()
    } match {
      case Success( rs ) =>
        val rows = rs.all().asScala
        rows.zipWithIndex.foreach{
          case ( row, i) =>
            val columns = row.getColumnDefinitions
            columns.asScala.foreach{ col =>
              val content = row.getBytesUnsafe( col.getName.toString )
              if( content != null )
                values.put( s"$i--"+col.getName.toString, JVBlob( content.array() ) )
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

  override def getKey( key: String ): String = {
    val tokenMap = session.getMetadata.getTokenMap
    val routingKey = TypeCodecs.TEXT.encode( key, session.getContext.getProtocolVersion )
    val hosts = if( ! tokenMap.isPresent )
      Set.empty[ UUID ]
    else
      tokenMap.get().getReplicas( keyspace, routingKey).asScala.map(_.getHostId).toSet
    hosts.toString
  }

  def read( op: Read )( key: String ): Status = {
    bulkRead( op )( List( key ) )
  }

  def bulkRead(op: Read)( keys: List[String]): Status = {
    Try {
      preparedStatementManager.getPrepared(op) match {
        case Some( prepared ) =>
          val result = new util.HashMap[String, ByteIterator]()
          val stmt = preparedStatementManager.bindRead(keys.distinct, prepared)
          readTo( keys.size, result, () => session.execute(stmt) )
        case None => throw new IllegalArgumentException( s" No prepared statements found for use case $op")
      }
    } match {
      case Failure( ex ) =>
        logger.error(s" Error in bulk read for ${op.toString} for $keys", ex)
        Status.ERROR
      case Success( status ) => status
    }
  }

  def update( op: Update )( entity: Entity ): Status = {
    write(op)( entity )
  }
  def bulkUpdate( op: Update )( entities: List[ Entity ] ): Status = {
    bulkWrite( op) ( entities )
  }
  def insert( op: Create )( entity: Entity ): Status = {
    write(op)( entity )
  }
  def bulkInsert( op: Create )( entities: List[ Entity ] ): Status = {
    bulkWrite( op) ( entities )
  }

  protected def write( op: UseCase )(entity: Entity ): Status = {
    Try {
      preparedStatementManager.getPrepared(op) match {
        case Some( prepared ) =>
          val stmt = preparedStatementManager.bindWrite( entity, prepared )
          persist( () => session.execute( stmt ) )
        case None => throw new IllegalArgumentException( s" No prepared statements found for use case $op")
      }
    } match {
      case Failure( ex ) =>
        logger.error(s" Error in write for ${op.toString} for $entity", ex)
        Status.ERROR
      case Success( status ) => status
    }
  }

  protected def bulkWrite( op: UseCase )( entities: List[ Entity ] ): Status = {
    Try {
      preparedStatementManager.getPrepared(op) match {
        case Some( prepared ) =>
          val stmt = BatchStatement.newInstance( DefaultBatchType.UNLOGGED )
          .addAll(
            entities.map{ entity =>
              preparedStatementManager.bindWrite( entity, prepared )
            }.asJava
          )
          persist( () => session.execute( stmt ) )
        case None => throw new IllegalArgumentException( s" No prepared statements found for use case $op")
      }
    } match {
      case Failure( ex ) =>
        logger.error(s" Error in write for ${op.toString} for ${entities}", ex)
        Status.ERROR
      case Success( status ) => status
    }
  }

  override def initGlobal(config: Map[String, String], schema: Schema, useCaseStore: UseCaseStore ): ScyllaPreparedStatementsStore = {
    val cql = ScyllaSession.build( config, useKeySpace = false )
    ScyllaSession.setup( schema, cql )
    new ScyllaPreparedStatementsStore( schema, useCaseStore, cql )}

  override def cleanupGlobal(): Unit = {
    preparedStatementManager.close()
  }
}
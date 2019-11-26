package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, PreparedStatement, ResultSet}
import com.joveox.ycsb.common.{ConfigManager, DBOperation, JVBlob, JoveoDBBatch, UseCase}
import com.yahoo.ycsb.{ByteIterator, Status}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ScyllaDB extends JoveoDBBatch with Logging {

  type PreparedStatementId = ByteBuffer
  type Hosts = Set[ UUID ]
  override type BatchKey = ( PreparedStatement, Hosts )

  protected var keyspace: String = _
  protected var session: CqlSession = _
  protected var preparedStatementManager: ScyllaUtils = _


  override def init(): Unit = {
    super.init()
    keyspace = ConfigManager.get.schema.db
    session = ScyllaDBSession.build( true )
    preparedStatementManager = ScyllaUtils.get
  }

  override def cleanup(): Unit = {
    super.cleanup()
  }

  protected def hosts( key: String ): Hosts = {
    val tokenMap = session.getMetadata.getTokenMap
    val routingKey = TypeCodecs.TEXT.encode( key, session.getContext.getProtocolVersion )
    if( ! tokenMap.isPresent )
      Set.empty[ UUID ]
    else
      tokenMap.get().getReplicas( keyspace, routingKey).asScala.map(_.getHostId).toSet
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
        if( rows.size < expected )
          logger.warn(s" Scylla expected $expected number of elems is less than returned ${rows.size} ")
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

  override protected def getKey(op: DBOperation.Value, id: String, useCase: UseCase ): (PreparedStatement, Hosts) = {
    val prepared = preparedStatementManager.get( op, useCase.involvedFields.asJava )
    val nodes = hosts( id )
    prepared -> nodes
  }

  override protected def bulkRead(op: UseCase)(ids: List[String]): Status = {
    Try {
      val prepared = preparedStatementManager.get(op.dbOperation, op.involvedFields.asJava )
      val result = new util.HashMap[String, ByteIterator]()
      val stmt = preparedStatementManager.bindRead(ids.distinct, prepared)
      readTo( ids.size, result, () => session.execute(stmt))
    } match {
      case Failure( ex ) =>
        logger.error(s" Error in bulk read for ${op.toString} for $ids", ex)
        Status.ERROR
      case Success( status ) => status
    }
  }

  override protected def bulkWrite(op: UseCase)(entities: List[(String, util.Map[String, ByteIterator])]): Status = {
    Try {
      val prepared = preparedStatementManager.get( op.dbOperation, op.involvedFields.asJava )
      entities match {
        case Nil => Status.ERROR
        case head :: Nil =>
          val stmt = preparedStatementManager.bindWrite( head._1, head._2, prepared )
          persist( () => session.execute( stmt ) )
        case _ =>
          val stmt = BatchStatement.newInstance( DefaultBatchType.UNLOGGED )
            .addAll(
              entities.map{ entity =>
                preparedStatementManager.bindWrite( entity._1, entity._2, prepared )
              }.asJava
            )
          persist( () => session.execute(stmt) )
      }
    } match {
      case Failure( ex ) =>
        logger.error(s" Error in bulk write for ${op.toString} for $entities", ex)
        Status.ERROR
      case Success( status ) => status
    }
  }
}
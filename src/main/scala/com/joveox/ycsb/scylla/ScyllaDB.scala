package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, PreparedStatement, ResultSet}
import com.joveox.ycsb.common.{ConfigManager, DBOperation, JVBlob, JoveoDBBatch, YCSBOperation}
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
    session = ScyllaDBSession.build( )
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

  override protected def getKey(op: DBOperation, id: String, operation: YCSBOperation): (PreparedStatement, Hosts) = {
    val prepared = preparedStatementManager.get( op, operation.fields.toSet.asJava )
    val nodes = hosts( id )
    prepared -> nodes
  }

  override protected def bulkRead(op: YCSBOperation)(ids: List[String]): Status = {
    val prepared = preparedStatementManager.get( op.operation, op.fields.toSet.asJava )
    val result = new util.HashMap[String, ByteIterator ]()
    val stmt = preparedStatementManager.bindRead( ids.toSet, prepared )
    readTo( result, () => session.execute( stmt ) )
  }

  override protected def bulkWrite(op: YCSBOperation)(entities: List[(String, util.Map[String, ByteIterator])]): Status = {
    val prepared = preparedStatementManager.get( op.operation, op.fields.toSet.asJava )
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
  }
}
package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.util

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._
import com.datastax.oss.driver.api.querybuilder.update.Assignment
import com.joveox.ycsb.common._
import com.yahoo.ycsb.ByteIterator

import scala.collection.JavaConverters._


object ScyllaUtils {

  private var instance: ScyllaUtils = _

  def init( schema: Schema, useCaseManager: UseCaseManager, session: CqlSession ): ScyllaUtils = {
    synchronized {
      if (instance == null) {
        instance = new ScyllaUtils( schema, useCaseManager, session )
      }
    }
    instance
  }

  def get: ScyllaUtils = instance

  def close(): Unit = {
    synchronized{
      if( instance != null ){
        instance.close()
        instance = null
      }
    }
  }
}


class ScyllaUtils( schema: Schema, useCaseManager: UseCaseManager, session: CqlSession ){

  def bindRead( keys: List[String], prepared: PreparedStatement ): BoundStatement = {
    prepared.bind( keys.asJava )
  }

  def bindWrite(key: String, values: util.Map[String, ByteIterator], prepared: PreparedStatement ): BoundStatement = {
    def extract( bit: ByteIterator  ): AnyRef = {
      bit match {
        case JVBoolean( v ) => java.lang.Boolean.valueOf( v )
        case JVByte( v ) => java.lang.Byte.valueOf( v )
        case JVShort( v ) => java.lang.Short.valueOf( v )
        case JVInt( v ) => java.lang.Integer.valueOf( v )
        case JVLong( v ) => java.lang.Long.valueOf( v )
        case JVFloat( v ) => java.lang.Float.valueOf( v )
        case JVDouble( v ) => java.lang.Double.valueOf( v )
        case JVText( v ) => v
        case JVBlob( v ) => ByteBuffer.wrap( v )
        case JVDate( v ) => v
        case JVTimestamp( v ) => v
      }
    }
    val fields = values.keySet().asScala.toList.sorted.map( f => extract( values.get( f ) ) ) ++ List( key )
    prepared.bind( fields:_*)
  }

  def close(): Unit = {
    session.close()
  }

  private def build(): Map[ ( DBOperation.Value, java.util.Set[String] ), PreparedStatement ] = {
    val operations = useCaseManager.useCases
    operations.map{ op =>
      val ( keyspace, table, key, fields ) = ( schema.db, schema.name, schema.primaryKey.name, op.involvedFields.toList.sorted )
      val stmt = op.dbOperation match {
        case DBOperation.CREATE => prepareInsert( keyspace, table, key, fields )
        case DBOperation.READ => prepareRead( keyspace, table, key, fields )
        case DBOperation.UPDATE => prepareUpdate( keyspace, table, key, fields )
      }
      ( op.dbOperation, fields.toSet.asJava ) ->
        ScyllaDBSession.retry(3, 1000, 2000,() => session.prepare( stmt ) )
    }.toMap
  }

  private val preparedStatementsByOperation = build()

  def get( dbOperation: DBOperation.Value, fields: java.util.Set[String] ): PreparedStatement = {
    preparedStatementsByOperation( ( dbOperation, fields ) )
  }

  private def prepareRead( db: String, table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    selectFrom( db, table )
      .columns( fields:_* )
      .whereColumn( keyField )
      .in( bindMarker( ) )
      .build()
  }

  private def prepareUpdate( db: String, table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    QueryBuilder.update( db, table )
      .set(
        fields.map{ col =>
          Assignment.setColumn( col, bindMarker( ))
        }:_*
      )
      .whereColumn( keyField )
      .isEqualTo( bindMarker( ) )
      .build()
  }

  private def prepareInsert( db: String, table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    val insert = insertInto( db, table )
    fields match {
      case Nil =>
        insert.value( keyField, bindMarker() ).build()
      case head:: rest  =>
        var stmt = insert.value( head, bindMarker() )
        rest.foreach{ field =>
          stmt = stmt.value( field, bindMarker() )
        }
        stmt.value( keyField, bindMarker() ).build()
    }
  }


}


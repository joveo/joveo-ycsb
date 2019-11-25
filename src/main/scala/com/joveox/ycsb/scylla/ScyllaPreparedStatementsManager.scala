package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.Properties

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, insertInto, selectFrom}
import com.datastax.oss.driver.api.querybuilder.update.Assignment
import com.joveox.ycsb.common._
import com.yahoo.ycsb.ByteIterator

import scala.collection.JavaConverters._


object ScyllaPreparedStatementsManager {

  private var instance: ScyllaPreparedStatementsManager = _

  def get( p: Properties ): ScyllaPreparedStatementsManager = {
    synchronized{
      if( instance == null ){
        val path = Paths.get( p.getProperty("scylla.use_cases") )
        val operationsManager = new YCSBOperationManager( path )
        val cql = ScyllaDBSession.build( ScyllaConf( p ) )
        instance = new ScyllaPreparedStatementsManager( operationsManager, cql )
      }
    }
    instance
  }

  def close(): Unit = {
    synchronized{
      if( instance != null ){
        instance.close()
        instance = null
      }
    }
  }

}


class ScyllaPreparedStatementsManager( operationManager: YCSBOperationManager, session: CqlSession ){

  def bindRead( keys: Set[String], prepared: PreparedStatement ): BoundStatement = {
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
    val fields = values.keySet().asScala.toList.sorted
    prepared.bind(
      (
        fields.map( f => extract( values.get( f ) ) ) :: List( key )
        ):_*
    )
  }

  def close(): Unit = {
    session.close()
  }

  private def build(): Map[ YCSBOperation, PreparedStatement ] = {
    val operations = operationManager.all
    operations.map{ op =>
      val stmt = op.operation match {
        case DBOperation.CREATE => prepareInsert( op.table, op.primaryKey, op.fields )
        case DBOperation.READ => prepareRead( op.table, op.primaryKey, op.fields )
        case DBOperation.UPDATE => prepareUpdate( op.table, op.primaryKey, op.fields )
      }
      op ->
        ScyllaDBSession.retry(3, 1000, 2000,() => session.prepare( stmt ) )
    }.toMap
  }

  private val preparedStatementsByOperation = build()

  def get( dbOperation: DBOperation, fields: java.util.Set[String] ): PreparedStatement = {
    val operation = operationManager.get( dbOperation, fields ) match {
      case Nil => throw new IllegalStateException(s"ScyllaPreparedStatementsManager: No prepared statement found for ($dbOperation,${fields.asScala.mkString(",")}) ")
      case head :: Nil => head
      case values => throw new IllegalStateException(
        s"ScyllaPreparedStatementsManager: More than 1 prepared statement found. Values ${
          values.map(_.toString()).mkString("\n")
        }"
      )
    }
    preparedStatementsByOperation( operation )
  }

  private def prepareRead( table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    selectFrom( table )
      .columns( fields:_* )
      .whereColumn( keyField )
      .in( bindMarker( keyField ) )
      .build()
  }

  private def prepareUpdate( table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    QueryBuilder.update( table )
      .set(
        fields.map{ col =>
          Assignment.setColumn( col, bindMarker())
        }:_*
      )
      .whereColumn( keyField )
      .isEqualTo( bindMarker( keyField ) )
      .build()
  }

  private def prepareInsert( table: String, keyField: String, fields: List[String] ): SimpleStatement = {
    var stmtBuilder = insertInto( table )
      .value( keyField, bindMarker( keyField ) )
    fields.foreach{ field =>
      stmtBuilder = stmtBuilder.value( field, bindMarker( field ) )
    }
    stmtBuilder.build()
  }

}


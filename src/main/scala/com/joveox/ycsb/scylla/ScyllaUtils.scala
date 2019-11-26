package com.joveox.ycsb.scylla

import java.nio.ByteBuffer
import java.util

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder._
import com.datastax.oss.driver.api.querybuilder.update.Assignment
import com.joveox.ycsb.common._
import com.yahoo.ycsb.ByteIterator

import scala.collection.JavaConverters._


object ScyllaUtils {

  private var instance: ScyllaUtils = _

  def init( schema: Schema, operationManager: YCSBOperationManager, session: CqlSession ): ScyllaUtils = {
    synchronized {
      if (instance == null) {
        instance = new ScyllaUtils( schema, operationManager, session )
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


class ScyllaUtils( schema: Schema, operationManager: YCSBOperationManager, session: CqlSession ){

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

  private def build(): Map[ ( DBOperation, java.util.Set[String] ), PreparedStatement ] = {
    val operations = operationManager.all
    operations.map{ op =>
      val stmt = op.operation match {
        case DBOperation.CREATE => prepareInsert( op.table, op.primaryKey, op.fields )
        case DBOperation.READ => prepareRead( op.table, op.primaryKey, op.fields )
        case DBOperation.UPDATE => prepareUpdate( op.table, op.primaryKey, op.fields )
      }
      ( op.operation, op.fields.toSet.asJava ) ->
        ScyllaDBSession.retry(3, 1000, 2000,() => session.prepare( stmt ) )
    }.toMap
  }

  private val preparedStatementsByOperation = build()

  def get( dbOperation: DBOperation, fields: java.util.Set[String] ): PreparedStatement = {
    preparedStatementsByOperation( ( dbOperation, fields ) )
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

  protected def tableDDL( field: Field, isPrimaryKey: Boolean = false ): String = {
    val scyllaType = field.`type` match {
      case FieldType.BOOLEAN => "boolean"
      case FieldType.BYTE => "tinyint"
      case FieldType.SHORT => "smallint"
      case FieldType.INT => "int"
      case FieldType.LONG => "bigint"
      case FieldType.FLOAT => "float"
      case FieldType.DOUBLE => "double"
      case FieldType.TEXT => "text"
      case FieldType.BLOB => "blob"
      case FieldType.DATE => "date"
      case FieldType.TIMESTAMP => "timestamp"
    }
    s" ${field.name} $scyllaType ${ if( isPrimaryKey) "PRIMARY KEY" else ""}"
  }

  protected def tableDDL( db: String, table: String ): String = {
    val key = tableDDL( schema.primaryKey, true )
    val innerFields = schema.fields.map{ field =>
      tableDDL( field )
    }

    s"""
       |CREATE TABLE IF NOT EXISTS $db.$table (
       |${( key :: innerFields ).mkString(",\n")}
       |) WITH compaction={'class':'LeveledCompactionStrategy'} AND compression = {'sstable_compression': 'LZ4Compressor'}
      """.stripMargin
  }

  def setup( ): Unit = {
    session.execute(
      createKeyspace( schema.db )
        .ifNotExists()
        .withSimpleStrategy( 2 )
        .build()
    ).wasApplied()

    val createTable = tableDDL( schema.db, schema.name )
    session.execute( createTable ).wasApplied()
  }

}


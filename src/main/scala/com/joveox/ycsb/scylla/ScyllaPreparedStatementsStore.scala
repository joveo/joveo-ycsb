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


class ScyllaPreparedStatementsStore(schemaStore: SchemaStore, useCaseStore: UseCaseStore, session: CqlSession ){

  private val reads = buildPrepare( useCaseStore.reads, prepareRead )
  private val updates = buildPrepare( useCaseStore.updates, prepareUpdate )
  private val inserts = buildPrepare( useCaseStore.inserts, prepareInsert )

  def buildPrepare[ T <: UseCase ]( useCases: List[T], fn: ( String, String, String, List[ String ] ) => SimpleStatement ): Map[ T, PreparedStatement ] = {
    useCases.map{ useCase =>
      useCase -> ScyllaSession.retry(
        3,
        1000, 2000,
        () => session.prepare( fn( schemaStore.db, useCase.schema, useCase.key.name, useCase.nonKeyFields.toList.sorted ) )
      )
    }.toMap
  }

  def getPrepared( useCase: UseCase ): Option[ PreparedStatement ] = {
    useCase match {
      case u: Read => reads.get( u )
      case u: Update => updates.get( u )
      case u: Create => inserts.get( u )
    }
  }

  def bindRead( keys: List[String], prepared: PreparedStatement ): BoundStatement = {
    prepared.bind( keys.asJava )
  }

  def bindWrite( entity: ( String, util.Map[String, ByteIterator] ), prepared: PreparedStatement ): BoundStatement = {
    //TODO: Use type coercion to match schema field type
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
          // TODO: Implement LIST and MAP
      }
    }
    val fields = entity._2.keySet().asScala.toList.sorted.map( f => extract( entity._2.get( f ) ) ) ++ List( entity._1 )
    prepared.bind( fields:_*)
  }

  def close(): Unit = {
    session.close()
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


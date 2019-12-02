package com.joveox.ycsb.scylla

import java.net.InetSocketAddress
import java.time.Duration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.joveox.ycsb.common._
import org.apache.logging.log4j.scala.Logging
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder._

import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._

import DataType._


object ScyllaSession extends Logging {

  @scala.annotation.tailrec
  def retry[T](retries: Int, waitMin: Int, waitMax: Int, run: () => T ): T = {
    Try{
      run()
    } match {
      case Success( value ) => value
      case Failure( ex ) =>
        logger.warn(s" ScyllaDB: Error while executing  ${ run.toString() }. Retries ${ retries } ", ex)
        if( retries > 0 ){
          if( waitMin > 0 )
            Thread.sleep( waitMin + ( if ( waitMax > waitMin) Random.nextInt( waitMax - waitMin ) else 0 ) )
          retry( retries -1, waitMin, waitMax, run )
        }
        else {
          logger.error(s" ScyllaDB: Error while executing  ${ run.toString() } ")
          throw ex
        }
    }
  }

  def build( config: Map[ String, String ], db: String, useKeySpace: Boolean ): CqlSession = {
    val conf = ScyllaConf(config)
    val keyspace = db
    var builder = CqlSession.builder()
    conf.hosts match {
      case None =>
        if (useKeySpace)
          builder = builder.withKeyspace(keyspace)
        builder.build()
      case Some(nodes) =>
        val hosts = nodes.split(",").map { host =>
          if (host.indexOf(':') > -1)
            new InetSocketAddress(host.split(":")(0), host.split(":")(1).toInt)
          else
            new InetSocketAddress(host.split(":")(0), 9042)
        }.toList
        builder = builder.addContactPoints(hosts.asJava)

        (conf.username, conf.password) match {
          case (Some(username), Some(password)) =>
            builder = builder.withAuthCredentials(username, password)
          case _ =>
        }

        conf.dataCenter match {
          case Some(dc) =>
            builder = builder.withLocalDatacenter(dc)
          case None =>
        }


        var configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
          .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1)
          .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1)

        conf.requestTimeoutMs match {
          case Some(time) if time > 0 =>
            configLoaderBuilder = configLoaderBuilder
              .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(time))
          case _ =>
        }

        builder = builder.withConfigLoader(configLoaderBuilder.build())
        if (useKeySpace)
          builder = builder.withKeyspace(keyspace)
        builder.build()
    }
  }



  protected def tableDDL( field: Field, isPrimaryKey: Boolean = false ): String = {

    val scyllaType = field.`type` match {
      case BOOLEAN => "boolean"
      case BYTE => "tinyint"
      case SHORT => "smallint"
      case INT => "int"
      case LONG => "bigint"
      case FLOAT => "float"
      case DOUBLE => "double"
      case TEXT => "text"
      case BLOB => "blob"
      case DATE => "date"
      case TIMESTAMP => "timestamp"
    }
    s" ${field.name} $scyllaType ${ if( isPrimaryKey) "PRIMARY KEY" else ""}"
  }

  protected def tableDDL( db: String, schema: Schema  ): String = {
    val key = tableDDL( schema.key, true )
    val innerFields = schema.fields.map{ field =>
      tableDDL( field )
    }

    s"""
       |CREATE TABLE IF NOT EXISTS $db.${schema.table} (
       |${( key :: innerFields ).mkString(",\n")}
       |) WITH compaction={'class':'LeveledCompactionStrategy'} AND compression = {'sstable_compression': 'LZ4Compressor'}
      """.stripMargin
  }

  def setup( db: String, schema: Schema, session: CqlSession ): Unit = {
    session.execute(
      createKeyspace( db )
        .ifNotExists()
        .withSimpleStrategy( 2 )
        .build()
    ).wasApplied()

    val createTable = tableDDL( db, schema )
    session.execute( createTable ).wasApplied()
  }



}

case class ScyllaConf(
                       hosts: Option[ String ] = None,
                       username: Option[ String ] = None,
                       password: Option[ String ] = None,
                       dataCenter: Option[String] = None,
                       requestTimeoutMs: Option[ Int ] = None
                     )

object ScyllaConf{
  def apply( config: Map[ String, String ]): ScyllaConf = {
    ScyllaConf(
      hosts = config.get("hosts"),
      username = config.get("username"),
      password = config.get("password"),
      dataCenter = config.get("data_center"),
      requestTimeoutMs = config.get("request_timeout").map( _.toInt )
    )
  }
}
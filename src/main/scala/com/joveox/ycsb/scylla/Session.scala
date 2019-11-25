package com.joveox.ycsb.scylla

import java.net.InetSocketAddress
import java.time.Duration
import java.util.Properties

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._



object ScyllaDBSession extends Logging {

  @scala.annotation.tailrec
  def retry[T](retries: Int, waitMin: Int, waitMax: Int, run: () => T ): T = {
    Try{
      run()
    } match {
      case Success( value ) => value
      case Failure( ex ) =>
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

  def build( conf: ScyllaConf ): CqlSession = {
    var builder = CqlSession.builder()
    conf.nodes match {
      case Nil =>
        builder.build()
      case nodes =>

        builder.addContactPoints( nodes.asJava )

        if( conf.username != null && conf.password != null )
          builder = builder.withAuthCredentials( conf.username, conf.password )

        if( conf.dataCenter != null )
          builder = builder.withLocalDatacenter( conf.dataCenter )

        val configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
          .withInt( DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1)
          .withInt( DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1)

        if( conf.requestTimeoutMs > 0 )
          configLoaderBuilder
            .withDuration( DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds( conf.requestTimeoutMs ) )

        builder = builder.withConfigLoader( configLoaderBuilder.build() )
        builder.withKeyspace( conf.keyspace )
        builder.build()

    }
  }

}

case class ScyllaConf(
                       nodes: List[ InetSocketAddress ],
                       username: String,
                       password: String,
                       dataCenter: String,
                       keyspace: String,
                       requestTimeoutMs: Int
                     )

object ScyllaConf {

  def apply( p: Properties ): ScyllaConf = {
    val hosts = p.getProperty("scylla.hosts")
    val username = p.getProperty( "scylla.username" )
    val password = p.getProperty( "scylla.password" )
    val dc = p.getProperty( "scylla.data_center" )
    val keyspace =  p.getProperty( "scylla.keyspace" )
    val requestTimeout = p.getProperty( "scylla.request_timeout", 5000.toString ).toInt
    val contactPoints = if( hosts != null ) {
      hosts.split(",").map { host =>
        if( host.indexOf(':') > -1 )
          new InetSocketAddress(host.split(":")(0), host.split(":")(1).toInt)
        else
          new InetSocketAddress(host.split(":")(0), 9042 )
      }.toList
    }
    else List.empty[ InetSocketAddress ]
    ScyllaConf(
      contactPoints,
      username,
      password,
      dc,
      keyspace,
      requestTimeout
    )
  }

}
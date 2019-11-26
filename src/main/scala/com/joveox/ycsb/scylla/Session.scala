package com.joveox.ycsb.scylla

import java.net.InetSocketAddress
import java.time.Duration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.joveox.ycsb.common.ConfigManager
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._
import pureconfig.generic.auto._



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

  def build(): CqlSession = {
    ConfigManager.get.db[ScyllaConf]("scylla") match {
      case Failure(ex) => throw ex
      case Success(conf) =>
        val keyspace = ConfigManager.get.schema.db
        var builder = CqlSession.builder()
        conf.hosts match {
          case None =>
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
            builder = builder.withKeyspace(keyspace)
            builder.build()
        }
    }
  }

}

case class ScyllaConf(
                       hosts: Option[ String ] = None,
                       username: Option[ String ] = None,
                       password: Option[ String ] = None,
                       dataCenter: Option[String] = None,
                       requestTimeoutMs: Option[ Int ] = None
                     )
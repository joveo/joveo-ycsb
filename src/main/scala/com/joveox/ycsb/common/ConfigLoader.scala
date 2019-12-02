package com.joveox.ycsb.common

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import pureconfig.generic.auto._
import pureconfig._


case class MockDBConf( console: Boolean = true, output: Path = Paths.get("/","tmp", "joveo.ycsb", "mock.db") )
case class DBConf( mockDBConf: MockDBConf, dbName: String, dbClass: String, config: Map[ String, String ] )




class ConfigManager( config: Config, isLoadPhase: Boolean ) extends Logging {

  private val source = ConfigSource.fromConfig( config )

  val dbConf = source.at("db").loadOrThrow[ DBConf ]
  val seed: SeedData = source.at("seed").loadOrThrow[ SeedData ]
  private val schemas: List[ Schema ] = source.at("schema").loadOrThrow[ List[ Schema ] ]
  val schemaStore = SchemaStore( dbConf.dbName, schemas )
  private val useCases = if( isLoadPhase ) {
    source.at("load-mode").loadOrThrow[ List[ Create]  ]
  } else{
    source.at("transactions-mode").load[ List[ UseCase ] ].getOrElse( List.empty )
  }

  val errors = useCases.map( u => u.name -> u.validate( schemaStore, seed ) ).filter(_._2.nonEmpty)
  if( errors.nonEmpty ){
    logger.error( errors.map(e => s" Errors in use case ${ e._1 }. Errors: \n ${e._2.mkString("\n") } ").mkString("\n\n") )
    System.exit( 10 )
  }

  val useCaseStore = UseCaseStore(
    useCases.filter( _.isInstanceOf[ Create] ).map( _.asInstanceOf[ Create ] ) ,
    useCases.filter( _.isInstanceOf[ Read] ).map( _.asInstanceOf[ Read ] ) ,
    useCases.filter( _.isInstanceOf[ Update ] ).map( _.asInstanceOf[ Update ] ) ,
  )

  def useCaseGenerator(threadId: Int, totalThreads: Int ): UseCaseGenerator = {
    val useCasesCopy = useCases.map( _.clone() )
    useCasesCopy.foreach( _.init( seed ) )
    UseCaseGenerator( threadId, totalThreads, useCasesCopy )
  }

}


object ConfigManager extends App{

  private var instance: ConfigManager = _

  def init(path: Path, isLoadPhase: Boolean ): ConfigManager = {
    synchronized{
      if( instance == null){
        instance = ConfigSource.file(path).config() match {
          case Left( error ) => throw new IllegalArgumentException(
            s" ERROR in loading config at $path. Description \n ${error.toList.map(_.description).mkString("\n")}"
          )
          case Right( config ) => new ConfigManager( config, isLoadPhase )
        }
      }
    }
    instance
  }

  def get: ConfigManager = instance

}
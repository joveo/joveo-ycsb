package com.joveox.ycsb.common

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config
import pureconfig.generic.auto._
import pureconfig._


case class MockDBConf( console: Boolean = true, output: Path = Paths.get("/","tmp", "joveo.ycsb", "mock.db") )
case class DBConf( mockDBConf: MockDBConf, dbClass: String, config: Map[ String, String ] )




class ConfigManager( config: Config, isLoadPhase: Boolean ) {

  private val source = ConfigSource.fromConfig( config )

  val dbConf = source.at("db").loadOrThrow[ DBConf ]
  val seed: SeedData = source.at("seed").loadOrThrow[ SeedData ]
  val schema: Schema = source.at("schema").loadOrThrow[ Schema ]
  private val useCases = if( isLoadPhase ) {
    List( source.at("load-mode").loadOrThrow[ Create ] )
  } else{
    source.at("transactions-mode").load[ List[ UseCase ] ].getOrElse( List.empty )
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
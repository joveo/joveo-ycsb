package com.joveox.ycsb.common

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config
import pureconfig.{ConfigReader, ConfigSource, Derivation}
import pureconfig.generic.auto._
import pureconfig._

import scala.reflect.ClassTag
import scala.util.Try


case class DBBatchConf( reads: Int = 30, updates: Int = 10, inserts: Int = 10 )
case class DBCommon( batch: DBBatchConf = DBBatchConf() )

case class MockDBConf( console: Boolean = true, output: Path = Paths.get("/","tmp", "joveo.ycsb", "mock.db") )



class ConfigManager( config: Config, isLoadPhase: Boolean ) {

  private val source = ConfigSource.fromConfig( config )

  private val dbSource = source.at("db")

  val dbCommon: DBCommon = dbSource.at( "common").load[ DBCommon ].getOrElse( DBCommon() )
  val mockDBCommon: MockDBConf = source.at( "mock").load[ MockDBConf ].getOrElse( MockDBConf() )

  def db[T: ClassTag]( name: String )(implicit reader: Derivation[ConfigReader[ T ]]): Try[ T ] = Try{
    dbSource.at( name ).loadOrThrow[T]
  }

  private val seed: SeedData = source.at("seed").loadOrThrow[ SeedData ]
  val schema: Schema = source.at("schema").loadOrThrow[ Schema ]
  private val load: Load = source.at("load").loadOrThrow[ Load ]
  private val transactions: List[ UseCase ] = source.at("transactions").load[ List[ UseCase ] ].getOrElse( List.empty )

  private val useCases = if( isLoadPhase ) List( load ) else transactions

  val useCaseStore = UseCaseStore( useCases )

  def useCaseIterator( threadId: Int, totalThreads: Int ): UseCaseIterator = {
    val useCasesCopy = useCases.map( _.copy() )
    useCasesCopy.foreach( _.init( seed ) )
    UseCaseIterator( threadId, totalThreads, useCasesCopy )
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
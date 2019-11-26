package com.joveox.ycsb.common

import java.nio.file.Path

import org.apache.logging.log4j.scala.Logging

import scala.io.Source

case class FieldLoader( id: String, path: Path, dir: Option[ Path ] = None )

case class SeedData(
                     dir: Path,
                     loaders: List[ FieldLoader ] = List.empty
                   ) extends Logging {

  private val delimiter = "\n##_##\n"

  private val seedsByField = loadAll()

  protected def load( path: Path ): Array[String] = {
    val source = Source.fromFile( path.toFile )
    val content = source.mkString.split( delimiter )
    source.close()
    content
  }

  protected def loadAll(): Map[String, Array[String]] = {
    logger.info("Loading seed data.")
    loaders.par.map{ loader =>
      val path = loader.dir.map( _.resolve( loader.path ) ).getOrElse( dir.resolve( loader.path ) )
      val data = load( path )
      logger.info(s"     Loaded seed data ${loader.id} found ${data.length} entries,  from $path")
      loader.id -> data
    }.toMap.seq
  }

  def getSeedData( id: String ): Array[ String ] = {
    seedsByField.getOrElse( id, Array.empty )
  }


}


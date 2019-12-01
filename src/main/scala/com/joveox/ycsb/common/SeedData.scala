package com.joveox.ycsb.common

import java.nio.file.{Files, Path}

import org.apache.logging.log4j.scala.Logging

import scala.io.Source

import scala.collection.JavaConverters._

case class FieldLoader( id: String, path: Path, dir: Option[ Path ] = None )

object SeedData{
  val delimiter = "\n##_##\n"
  def save( path: Path, values: Array[ String ] ): Unit = {
    val writer = Files.newBufferedWriter( path )
    values.zipWithIndex.foreach{
      case ( value, idx ) =>
      writer.write( value )
        if( idx != ( values.length - 1) )
          writer.write( delimiter )
    }
    writer.flush()
    writer.close()
  }
}

case class SeedData(
                     dir: Path,
                     loaders: List[ FieldLoader ] = List.empty
                   ) extends Logging {

  private val seedsByField = loadAll()

  protected def load( path: Path ): Array[String] = {
    if( path.toFile.isDirectory ) {
      Files.list( path ).iterator().asScala.toList.par
        .flatMap( load )
        .toArray
    } else{
      val source = Source.fromFile( path.toFile )
      val content = source.mkString.split( SeedData.delimiter )
      source.close()
      content
    }

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


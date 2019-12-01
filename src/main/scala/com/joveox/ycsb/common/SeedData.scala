package com.joveox.ycsb.common

import java.nio.file.{Files, Path}
import java.util.Scanner

import org.apache.logging.log4j.scala.Logging

import scala.collection.AbstractIterator
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
  def saveAll( path: Path, content: Array[ String ], grouped: Int ): Unit = {
    Files.createDirectories( path )
    val batched = content.grouped( grouped ).toArray
    batched.zipWithIndex.par.foreach{  case ( values, batchId )  =>
      save( path.resolve(batchId+".out"), values )
    }
  }

  def load( path: Path ): Array[ String ] = {
    val scanner = new Scanner( Source.fromFile( path.toFile, 1024*1024 ).bufferedReader() )
    scanner.useDelimiter( SeedData.delimiter )
    val it = new AbstractIterator[ String ] {
      override def hasNext = scanner.hasNext
      override def next() = scanner.next()
    }
    val content = it.toArray
    scanner.close()
    content
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
      SeedData.load( path )
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
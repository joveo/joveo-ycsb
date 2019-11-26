package com.joveox.ycsb.common

import java.nio.file.Path

import enumeratum._
import org.apache.logging.log4j.scala.Logging

import scala.io.Source

sealed trait FieldType extends EnumEntry
object FieldType extends Enum[FieldType] {

  val values = findValues

  case object BOOLEAN extends FieldType
  case object BYTE extends FieldType
  case object SHORT extends FieldType
  case object INT extends FieldType
  case object LONG extends FieldType
  case object FLOAT extends FieldType
  case object DOUBLE extends FieldType
  case object TEXT extends FieldType
  case object BLOB extends FieldType
  case object DATE extends FieldType
  case object TIMESTAMP extends FieldType
}

case class Field(name: String, `type`: FieldType, generator: FieldGeneratorType )

case class FieldLoader( name: String, path: Path )
case class SeedData(
                     dir: Path,
                     loaders: List[ FieldLoader ] = List.empty
                   )



case class Schema(
                   db: String,
                   name: String,
                   seed: SeedData,
                   primaryKey: Field,
                   fields: List[ Field ] = List.empty
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
    seed.loaders.par.map{ loader =>
      val path = seed.dir.resolve( loader.path )
      val data = load( path )
      logger.info(s"     Loaded seed data ${loader.name} found ${data.length} entries,  from $path")
      loader.name -> data
    }.toMap.seq
  }

  def getSeedData( field: String ): Array[ String ] = {
    seedsByField.getOrElse( field, Array.empty )
  }

  def allFields: List[ Field ] = primaryKey :: fields

}


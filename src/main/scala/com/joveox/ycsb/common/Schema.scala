package com.joveox.ycsb.common

import java.nio.file.Path

import enumeratum._

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

case class Field(name: String, `type`: FieldType, generator: FieldGeneratorType, isPrimaryKey: Boolean = false )

case class FieldLoader( name: String, path: Path )
case class SeedData(
                     dir: Path,
                     loaders: List[ FieldLoader ]
                   )



case class Schema(
                   name: String,
                   seed: SeedData,
                   fields: List[ Field ]
                 ){

  private val delimiter = "\n##_##\n"

  private var seedsByField = Map.empty[ String, Array[ String ] ]

  val primaryKey: Field = fields.filter(_.isPrimaryKey).head

  protected def load( path: Path ): Array[String] = {
    val source = Source.fromFile( path.toFile )
    val content = source.mkString.split( delimiter )
    source.close()
    content
  }

  protected def loadAll(): Map[String, Array[String]] = {
    seed.loaders.map{ loader =>
      val path = seed.dir.resolve( loader.path )
      val data = load( path )
      loader.name -> data
    }.toMap
  }

  def init(): Unit = {
    seedsByField = loadAll()
  }

  def getSeedData( field: String ): Array[ String ] = {
    seedsByField.getOrElse( field, Array.empty )
  }

}


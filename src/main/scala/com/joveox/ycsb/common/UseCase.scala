package com.joveox.ycsb.common

import java.nio.file.{Files, Path}
import java.util

import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import enumeratum._

import scala.collection.{AbstractIterator, mutable}
import scala.collection.JavaConverters._
import scala.util.Random

sealed trait DBOperation extends EnumEntry
object DBOperation extends Enum[DBOperation] {
  val values = findValues
  case object CREATE   extends DBOperation
  case object READ extends DBOperation
  case object UPDATE      extends DBOperation
  case object DELETE     extends DBOperation
  case object SCAN     extends DBOperation
}

case class UseCaseField( name: String, generator: FieldGenerator, threadUnique: Boolean = false ){
  def next( threadId: Int, idx: Int ): JvByteIterator = {
    val value = generator.next( threadId, idx )
    val transformed = if( threadUnique ){
      value match {
        case JVText(underlying) => JVText( s"$threadId--$idx--" + underlying )
        case JVBlob(underlying) => JVBlob( s"$threadId--$idx--".getBytes ++ underlying )
        case _ => value
      }
    } else value
    transformed
  }
}

sealed trait UseCase {
  val dbOperation: DBOperation
  val name: String
  val load: Int
  val batchSize: Int
  val schema: String
  val key: UseCaseField

  def init( seed: SeedData ): Unit

  def runNext( db: DB, schema: Schema, threadId: Int, idx: Int ): Status

  def nextKey( threadId: Int, idx: Int ): String = key.next( threadId, idx ).toString

  val nonKeyFields: Set[ String ]

  def cleanup() : Unit  = ()

  def validate( schemaStore: SchemaStore, seedData: SeedData ): List[ String ] = {
    val schemaExists = schemaStore.schemas.exists( _.table == schema )
    val keyFieldValid = if( schemaExists ) schemaStore.get( schema ).key.name == key.name else true
    val fieldsInSchema = if( schemaExists )  schemaStore.get( schema ).fields.map(_.name).toSet else Set.empty[ String ]
    val fieldsNotFound =  if( schemaExists ) nonKeyFields diff fieldsInSchema else Set.empty[ String ]
    List(
      (  if( ! schemaExists ) Some( s" Undefined  schema $schema. " ) else None ),
      (  if( ! keyFieldValid ) Some( s" Key field ${key.name}  does not match key name in schema $schema." ) else None ),
      (  if( fieldsNotFound.nonEmpty ) Some( s" Undefined  fields ${fieldsNotFound.mkString(",")} in $schema." ) else None ),
    ).flatten

  }

  override def clone( ) : UseCase = {
    this match {
      case v : Read => v.copy()
      case write: Write =>
        write match {
          case v: Create => v.copy()
          case v: Update=> v.copy()
       }
    }
  }
}

case class Read( name: String, load: Int, key: UseCaseField, nonKeyFields: Set[ String ], schema: String, batchSize: Int = 1 ) extends UseCase {

  val dbOperation = DBOperation.READ

  def init( seed: SeedData ): Unit = {
    key.generator.init( seed )
  }

  override def runNext(db: DB, schema: Schema, threadId: Int, idx: Int): Status = {
    val keyValue = nextKey( threadId, idx )
    val result = new java.util.HashMap[ String, ByteIterator ]
    db.read( schema.table, keyValue, nonKeyFields.asJava, result )
  }

}

sealed trait Write extends UseCase {

  val fields: List[ UseCaseField ]

  def run( db: DB, table: String, key: String, values: java.util.Map[ String, ByteIterator ] ): Status

  def init( seed: SeedData ): Unit = {
    ( key.generator :: fields.map( _.generator ) ).foreach( _.init( seed ))
  }

  override def runNext(db: DB, schema: Schema, threadId: Int, idx: Int): Status = {
    val keyValue = nextKey( threadId, idx )
    val values = fields.map{ field =>
      field.name -> field.generator.next( threadId, idx ).asInstanceOf[ ByteIterator ]
    }.toMap.asJava

    run( db, schema.table, keyValue, values )
  }

  override val nonKeyFields: Set[String] = fields.map(_.name).toSet

  override def validate(schemaStore: SchemaStore, seedData: SeedData): List[String] = {
    val errors = super.validate(schemaStore, seedData)
    val seedDataAvailableFor = seedData.loaders.map( _.id ).toSet
    val seedDataNotFound = fields.map( _.generator ).collect{
      case v: SeedGenerator => v.id
    }.toSet diff seedDataAvailableFor

    val seedDataErrors = if( seedDataNotFound.nonEmpty )
      List( s" Undefined  fields ${seedDataNotFound.mkString(",")} in $schema." )
    else
      List.empty[ String ]
    seedDataErrors ++ errors
  }

}

case class Create( name: String, load: Int, key: UseCaseField,  fields: List[ UseCaseField ], schema: String, batchSize: Int = 1, persistKeys: Boolean, outputPath: Option[ Path ]  ) extends Write {

  val dbOperation = DBOperation.CREATE

  outputPath match {
    case Some( path ) =>
      if( ! path.toFile.exists() )
        Files.createDirectories( path )
      assert( path.toFile.isDirectory, " Create use case needs the output path to be a directory " )
    case None =>
  }

  private val id = Random.nextInt( 100000 ).toString

  private val keys = mutable.HashSet.empty[ String ]

  override def run(db: DB, table: String, keyValue: String, values: util.Map[String, ByteIterator]): Status = {
    val status = db.insert( table, keyValue, values )
    if( status.isOk )
      keys += keyValue
    status
  }

  override def cleanup(): Unit = {
    super.cleanup()
    if( persistKeys ){
      outputPath match {
        case Some( path ) =>
          val writeTo = path.resolve( key.name+s".$id.ycsb.out/" )
          SeedData.save( writeTo, keys.toArray )
        case None =>
      }
    }
  }


}
case class Update( name: String, load: Int, key: UseCaseField,  fields: List[ UseCaseField ], schema: String, batchSize: Int = 1 ) extends Write {

  val dbOperation = DBOperation.READ

  override def run(db: DB, table: String, keyValue: String, values: util.Map[String, ByteIterator]): Status = db.update( table, keyValue, values )

}

case class UseCaseStore( inserts: List[ Create ], reads: List[ Read ], updates: List[ Update ] ){

  private val readsByFields = reads
    .groupBy( _.nonKeyFields )
    .map( kv => kv._1 -> kv._2.head )

  private val insertsByFields = inserts
    .groupBy( _.nonKeyFields )
    .map( kv => kv._1 -> kv._2.head )

  private val updatesByFields = updates
    .groupBy( _.nonKeyFields )
    .map( kv => kv._1 -> kv._2.head )


  def read( fields: java.util.Set[String] ): Option[ Read ] =  readsByFields.get( fields.asScala.toSet )
  def update( fields: java.util.Set[String] ): Option[ Update ] =  updatesByFields.get( fields.asScala.toSet )
  def insert( fields: java.util.Set[String] ): Option[ Create ] =  insertsByFields.get( fields.asScala.toSet )

  def all: List[ UseCase ] = inserts ++ updates ++ reads
}

case class UseCaseGenerator(threadId: Int, totalThreads: Int, useCases: List[ UseCase ] ) extends AbstractIterator[ UseCase ]{

  private val useCaseByName = useCases.groupBy( _.name ).map( kv => kv._1 -> kv._2.head )
  private val generator = new DiscreteGenerator()

  useCases.foreach{ useCase =>
    generator.addValue( useCase.load.toDouble, useCase.name )
  }

  private var currentIdx = -1

  def idx: Int = currentIdx

  override def hasNext: Boolean = true

  override def next(): UseCase = {
    currentIdx = currentIdx + 1
    useCaseByName( generator.nextValue() )
  }

  def cleanup(): Unit = {
    useCases.foreach( _.cleanup() )
  }
}
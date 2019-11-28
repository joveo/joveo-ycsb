package com.joveox.ycsb.common

import java.nio.file.Path
import java.util

import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import enumeratum._
import org.apache.logging.log4j.scala.Logging

import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

sealed trait DBOperation extends EnumEntry
object DBOperation extends Enum[DBOperation] {
  val values = findValues
  case object CREATE   extends DBOperation
  case object READ extends DBOperation
  case object UPDATE      extends DBOperation
  case object DELETE     extends DBOperation
  case object SCAN     extends DBOperation
}

case class UseCaseField( name: String, generator: FieldGenerator, threadUnique: Boolean = false )

sealed trait UseCase {
  val dbOperation: DBOperation
  val name: String
  val load: Int
  val key: UseCaseField
  def init( seed: SeedData ): Unit
  def runNext( db: DB, schema: Schema, threadId: Int, idx: Int ): Status
  def nextKey( threadId: Int, idx: Int ): String = key.generator.next( threadId, idx ).toString
  val nonKeyFields: Set[ String ]

  def cleanup() : Unit  = ()

  def copy( ) : UseCase = {
    this match {
      case v : Read => v.copy()
      case write: Write =>
        write match {
          case v: Create => v.copy()
          case v: Update=> v.copy()
          case v: Load => v.copy()
        }
    }
  }
}

case class Read( name: String, load: Int, key: UseCaseField, nonKeyFields: Set[ String ]  ) extends UseCase {

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

}

case class Create( name: String, load: Int, key: UseCaseField,  fields: List[ UseCaseField ] ) extends Write {

  val dbOperation = DBOperation.CREATE

  override def run(db: DB, table: String, keyValue: String, values: util.Map[String, ByteIterator]): Status = db.insert( table, keyValue, values )

}
case class Update( name: String, load: Int, key: UseCaseField,  fields: List[ UseCaseField ] ) extends Write {

  val dbOperation = DBOperation.READ

  override def run(db: DB, table: String, keyValue: String, values: util.Map[String, ByteIterator]): Status = db.update( table, keyValue, values )

}

case class Load( name: String, load: Int, key: UseCaseField,  fields: List[ UseCaseField ], persistKeys: Boolean, outputPath: Option[ Path ] )
  extends Write {
  val dbOperation = DBOperation.CREATE

  override def run(db: DB, table: String, keyValue: String, values: util.Map[String, ByteIterator]): Status = {
    db.insert( table, keyValue, values )
  }

  override def cleanup(): Unit = {
    super.cleanup()
  }

}

case class UseCaseStore( useCases: List[ UseCase ] ){

  private val useCasesByPayload = useCases.groupBy( op => ( op.dbOperation, op.nonKeyFields )  )
    .map( kv => kv._1 -> kv._2.head )

  def get( operation: DBOperation, fields: java.util.Set[String] ): Option[ UseCase ] = {
    useCasesByPayload.get( ( operation, fields.asScala.toSet ) )
  }
}

case class UseCaseIterator( threadId: Int, totalThreads: Int, useCases: List[ UseCase ] ) extends AbstractIterator[ UseCase ]{

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
}
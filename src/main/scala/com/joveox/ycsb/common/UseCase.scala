package com.joveox.ycsb.common

import java.util

import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import org.apache.logging.log4j.scala.Logging

import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

object DBOperation extends Enumeration{
  val CREATE, READ, UPDATE, DELETE, SCAN = Value
}

case class UseCaseField( name: String, generator: FieldGenerator[ _ ], threadUnique: Boolean = false )

sealed trait UseCase{
  val dbOperation: DBOperation.Value
  val name: String
  val load: Int
  val keyGenerator: FieldGenerator[ String ]
  def init( seed: SeedData ): Unit
  def runNext( db: DB, schema: Schema, threadId: Int, idx: Int ): Status
  def nextKey( threadId: Int, idx: Int ): String = keyGenerator.next( threadId, idx )
  val involvedFields: Set[ String ]
}

case class Read( name: String, load: Int, keyGenerator: FieldGenerator[ String ], fields: Set[ String ]  ) extends UseCase {

  val dbOperation = DBOperation.READ

  def init( seed: SeedData ): Unit = {
    keyGenerator.init( seed )
  }

  override def runNext(db: DB, schema: Schema, threadId: Int, idx: Int): Status = {
    val key = nextKey( threadId, idx )
    val result = new java.util.HashMap[ String, ByteIterator ]
    db.read( schema.name, key, fields.toSet.asJava, result )
  }

  override val involvedFields: Set[String] = fields
}

sealed trait Write extends UseCase {

  val fields: List[ UseCaseField ]

  def run( db: DB, table: String, key: String, values: java.util.Map[ String, ByteIterator ] ): Status

  def init( seed: SeedData ): Unit = {
    ( keyGenerator :: fields.map( _.generator ) ).foreach( _.init( seed ))
  }

  override def runNext(db: DB, schema: Schema, threadId: Int, idx: Int): Status = {
    val key = nextKey( threadId, idx )
    val values = fields.map{ field =>
      field.name -> field.generator.nextByteIterator( threadId, idx ).asInstanceOf[ ByteIterator ]
    }.toMap.asJava

    run( db, schema.name, key, values )
  }

  override val involvedFields: Set[String] = fields.map(_.name).toSet

}

case class Create( name: String, load: Int, keyGenerator: FieldGenerator[ String ],  fields: List[ UseCaseField ] ) extends Write {

  val dbOperation = DBOperation.CREATE

  override def run(db: DB, table: String, key: String, values: util.Map[String, ByteIterator]): Status = db.insert( table, key, values )

}
case class Update( name: String, load: Int, keyGenerator: FieldGenerator[ String ],  fields: List[ UseCaseField ] ) extends Write {

  val dbOperation = DBOperation.READ

  override def run(db: DB, table: String, key: String, values: util.Map[String, ByteIterator]): Status = db.update( table, key, values )

}


case class UseCaseManager( schema: Schema, seed: SeedData, useCases: List[ UseCase ] ) extends Logging {

  useCases.foreach( _.init( seed ))

  logger.info(s" Initialized all following operation:\n${useCases.mkString("\n")}")

  private val useCasesByPayload = useCases.groupBy( op => ( op.dbOperation, op.involvedFields )  )
    .map( kv => kv._1 -> kv._2.head )

  def get( operation: DBOperation.Value, fields: java.util.Set[String] ): Option[ UseCase ] = {
    useCasesByPayload.get( ( operation, fields.asScala.toSet ) )
  }

  def iterator( threadId: Int, totalThreads: Int ): UseCaseIterator = {
    new UseCaseIterator( threadId, threadId, useCases )
  }

}

class UseCaseIterator(val threadId: Int, val totalThreads: Int, useCases: List[ UseCase ] ) extends AbstractIterator[ UseCase ]{

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
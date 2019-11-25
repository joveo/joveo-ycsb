package com.joveox.ycsb.common

import java.nio.file.{Path, Paths}
import java.util.Properties

import com.joveox.ycsb.scylla.{ScyllaConf, ScyllaDBSession, ScyllaUtils}
import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import enumeratum._
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

sealed trait DBOperation extends EnumEntry
object DBOperation extends Enum[DBOperation] {

  val values = findValues

  case object CREATE extends DBOperation
  case object READ extends DBOperation
  case object UPDATE extends DBOperation
  case object DELETE extends DBOperation
  case object SCAN extends DBOperation

}

case class UseCase( name: String, operation: DBOperation, load: Float, fields: List[String]  )


case class YCSBOperation( useCase: UseCase, schema: Schema ){

  private val recordGenerator = new RecordGenerator( schema )

  val table: String = schema.name
  val primaryKey: String = schema.primaryKey.name
  val name: String = useCase.name
  val operation: DBOperation = useCase.operation
  val load: Float = useCase.load
  val fields: List[String] = useCase.fields.sorted

  def init(): Unit = {
    recordGenerator.init()
  }

  def runNext( db: DB ): Status = {
    val key = recordGenerator.nextKey()
    useCase.operation match {
      case DBOperation.CREATE =>
        val values = recordGenerator.nextFields( useCase.fields:_* )
        db.insert( table, key, values )
      case DBOperation.READ =>
        val result = new java.util.HashMap[ String, ByteIterator ]
        db.read( table, key, fields.toSet.asJava, result )
      case DBOperation.UPDATE =>
        val values = recordGenerator.nextFields( useCase.fields:_* )
        db.update( table, key, values )
      case DBOperation.DELETE =>
        db.delete( schema.name, key )
    }
  }
}

class YCSBOperationManager( path: Path, val isLoad: Boolean ){

  private val config = ConfigSource.file( path )

  val schema: Schema = config.at("schema").loadOrThrow[ Schema ]
  schema.init()
  private val transactional = config.at("use_cases").loadOrThrow[ List[ UseCase] ]

  private val insertOnly = config.at("load").loadOrThrow[ UseCase ]

  private val useCases: List[UseCase] = if( isLoad ) List( insertOnly ) else transactional

  val operations: List[YCSBOperation] = useCases.map { useCase =>
    val op = YCSBOperation(useCase, schema)
    op.init()
    op
  }
  private val operationsByName = operations.groupBy( _.name ).map( kv => kv._1 -> kv._2.head )
  private val operationsByPayload = operations.groupBy( op => ( op.operation, op.fields )  )
    .map( kv => kv._1 -> kv._2 )


  def all: List[ YCSBOperation ] = operations
  def getSafe( name: String ): Option[ YCSBOperation ] = operationsByName.get( name )
  def get( name: String ): YCSBOperation = operationsByName( name )
  def get( operation: DBOperation, fields: java.util.Set[String] ): List[ YCSBOperation ] = {
    operationsByPayload.getOrElse( ( operation, fields.asScala.toList.sorted ), List.empty )
  }

  def iterator( threadId: Int, totalThreads: Int ): Iterator[ YCSBOperation ] = {
    new AbstractIterator[YCSBOperation] {
      private val generator = new DiscreteGenerator()
      all.foreach{ useCase =>
        generator.addValue( useCase.load.toDouble, useCase.name )
      }
      override def hasNext: Boolean = true

      override def next(): YCSBOperation = operationsByName( generator.nextValue() )
    }
  }

}


object YCSBOperationManager {

  private var instance: YCSBOperationManager = _

  def init( p: Properties, isLoad: Boolean ): YCSBOperationManager = {
    synchronized{
      if( instance == null ){
        val path = Paths.get( p.getProperty("joveo.use_cases") )
        instance = new YCSBOperationManager( path, isLoad )
      }
    }
    instance
  }

  def get: YCSBOperationManager = instance

}

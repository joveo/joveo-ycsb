package com.joveox.ycsb.common

import java.nio.file.Path

import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import enumeratum._
import pureconfig.ConfigSource
import pureconfig.generic.auto._

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

class YCSBOperationManager( path: Path ){

  private val config = ConfigSource.file( path )

  private val schema = config.at("schema").loadOrThrow[ Schema ]
  schema.init()
  private val useCases = config.at("use_cases").loadOrThrow[ List[ UseCase] ]

  private val operations = useCases.map { useCase =>
    val op = YCSBOperation(useCase, schema)
    op.init()
    op
  }
  private val operationsByName = operations.groupBy( _.name ).map( kv => kv._1 -> kv._2.head )
  private val operationsByPayload = operations.groupBy( op => ( op.operation, op.fields )  )
    .map( kv => kv._1 -> kv._2 )
  private val generator = new DiscreteGenerator()
  all.foreach{ useCase =>
    generator.addValue( useCase.load.toDouble, useCase.name )
  }

  def all: List[ YCSBOperation ] = operations
  def getSafe( name: String ): Option[ YCSBOperation ] = operationsByName.get( name )
  def get( name: String ): YCSBOperation = operationsByName( name )
  def get( operation: DBOperation, fields: java.util.Set[String] ): List[ YCSBOperation ] = {
    operationsByPayload.getOrElse( ( operation, fields.asScala.toList.sorted ), List.empty )
  }
  def next(): YCSBOperation = {
    val useCase = generator.nextValue()
    operationsByName( useCase )
  }

}


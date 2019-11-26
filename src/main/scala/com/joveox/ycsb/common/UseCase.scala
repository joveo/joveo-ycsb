package com.joveox.ycsb.common

import com.yahoo.ycsb.{ByteIterator, DB, Status}
import com.yahoo.ycsb.generator.DiscreteGenerator
import enumeratum._
import org.apache.logging.log4j.scala.Logging
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

case class UseCase(name: String, dbOp: DBOperation, load: Float, fields: List[String]  )


case class YCSBOperation( useCase: UseCase, schema: Schema ) extends Logging {

  private val recordGenerator = new RecordGenerator( schema )

  val table: String = schema.name
  val primaryKey: String = schema.primaryKey.name
  val name: String = useCase.name
  val operation: DBOperation = useCase.dbOp
  val load: Float = useCase.load
  val fields: List[String] = useCase.fields.sorted

  def runNext( db: DB, threadId: Int, idx: Int ): Status = {
    val key = recordGenerator.nextKey( threadId, idx )
    useCase.dbOp match {
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

  override def toString: String = {
    s"name=$name, table=$table, primaryKey=$primaryKey, operation=${operation.toString}, load=$load, fields=${fields.mkString("__")}"
  }
}

class YCSBOperationManager( schema: Schema, useCases: List[ UseCase] ) extends Logging {

  val operations: List[YCSBOperation] = useCases.map { useCase =>
    YCSBOperation(useCase, schema)
  }

  logger.info(s" Initialized all following operation:\n${operations.mkString("\n")}")
  private val operationsByName = operations.groupBy( _.name ).map( kv => kv._1 -> kv._2.head )
  private val operationsByPayload = operations.groupBy( op => ( op.operation, op.fields )  )
    .map( kv => kv._1 -> kv._2 )


  def all: List[ YCSBOperation ] = operations
  def getSafe( name: String ): Option[ YCSBOperation ] = operationsByName.get( name )
  def get( name: String ): YCSBOperation = operationsByName( name )
  def get( operation: DBOperation, fields: java.util.Set[String] ): List[ YCSBOperation ] = {
    operationsByPayload.getOrElse( ( operation, fields.asScala.toList.sorted ), List.empty )
  }

  def iterator( threadId: Int, totalThreads: Int ): OperationIterator = {
    new OperationIterator( threadId, threadId, operationsByName )
  }

}

class OperationIterator( val threadId: Int, val totalThreads: Int, operations: Map[ String, YCSBOperation ] ) extends AbstractIterator[ YCSBOperation ]{
  private val generator = new DiscreteGenerator()
  operations.valuesIterator.foreach{ useCase =>
    generator.addValue( useCase.load.toDouble, useCase.name )
  }

  private var currentIdx = -1

  def idx: Int = currentIdx

  override def hasNext: Boolean = true

  override def next(): YCSBOperation = {
    currentIdx = currentIdx + 1
    operations( generator.nextValue() )
  }
}
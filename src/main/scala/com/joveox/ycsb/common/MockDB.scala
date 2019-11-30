package com.joveox.ycsb.common

import java.io.{BufferedWriter, FileWriter, Writer}
import java.nio.file.{Files, Paths}

import com.yahoo.ycsb.{ByteIterator, Status}
import org.apache.logging.log4j.scala.Logging
import java.util
import java.util.Base64

import scala.util.Random
import scala.collection.JavaConverters._



class MockDB extends DBExtension with  Logging {

  private var db : Writer = _

  private val id = Random.alphanumeric.take(10).mkString("")

  override def cleanup(): Unit = {
    db.flush()
    db.close()
  }


  override def init( config: Map[ String, String ], schema: Schema, global: Any ): Unit = {
    val conf = config
    val root = Paths.get( conf( "output ") )
    if( ! root.toFile.exists() )
      Files.createDirectories( root )
    val dbPath = root.resolve( id + ".txt"  )
    db = new BufferedWriter(new FileWriter( dbPath.toFile))
  }

  private def serialize( result: util.Map[String, ByteIterator] ): String = {
    def sanitize( s: String ) = if( s.length < 20) s else ( s.take( 20 ) + "... ")
    result.asScala.map {
      case ( key, value ) =>
        val valueStr = value match {
          case JVBoolean(underlying) =>  underlying.toString
          case JVByte(underlying) => underlying.toString
          case JVShort(underlying) => underlying.toString
          case JVInt(underlying) => underlying.toString
          case JVLong(underlying) => underlying.toString
          case JVFloat(underlying) => underlying.toString
          case JVDouble(underlying) => underlying.toString
          case JVText(underlying) => sanitize( underlying )
          case JVBlob(underlying) => s" [base-64 size=${underlying.length}] content="+ sanitize( Base64.getEncoder.encodeToString( underlying ) )
          case JVDate(underlying) => underlying.toString
          case JVTimestamp(underlying) => underlying.toString
          case JVList( values ) => values.map( _.underlying.toString ).mkString("::")
          case JVMap( values ) => values.map( kv => s"${kv._1.toString}:${kv._2.toString}")
          case _ => "UNKNOWN"
        }
        s"$key=$valueStr"
    }.mkString(",")
  }



  private def log ( content: String ): Status = {
    db.write(content)
    logger.info( content )
    Status.OK
  }

  override def initGlobal(config: Map[String, String], schema: Schema, useCaseStore: UseCaseStore): Any = ()

  override def read(op: Read)(key: String): Status = {
    log( s"op=READ, keys=$key, fields=${op.nonKeyFields.mkString("::")},null\n")
  }

  override def bulkRead(op: Read)(keys: List[String]): Status = {
    log( s"op=READ, keys=${keys.mkString(",")}, fields=${op.nonKeyFields.mkString("::")},null\n")
  }

  protected def write(op: UseCase)(entity: Entity ): Status = {
    val fields = op.nonKeyFields.mkString("::")
    val elems = serialize( entity._2 )
    log( s"op=${op.dbOperation}, keys=${entity._1}, fields=$fields,elems=\n$elems\n")
  }

  protected def bulkWrite(op: UseCase)(entities: List[ Entity ]): Status = {
    val keys = entities.map(_._1).mkString("::")
    val fields = op.nonKeyFields.mkString("::")
    val elems = entities.map(_._2).map( serialize ).mkString("\n")
    log( s"op=${op.dbOperation}, keys=$keys, fields=$fields,elems=\n$elems\n")
  }

  override def update(op: Update)(entity: Entity ): Status = {
    write( op )( entity )
  }

  override def bulkUpdate(op: Update)(entities: List[ Entity ]): Status = {
    bulkWrite( op )( entities )
  }

  override def insert(op: Create)(entity: Entity): Status = {
    write( op )( entity )
  }

  override def bulkInsert(op: Create)(entities: List[ Entity ]): Status = {
    bulkWrite( op )( entities )
  }

  override def cleanupGlobal(): Unit = ()
}
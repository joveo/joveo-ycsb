package com.joveox.ycsb.common

import java.io.{BufferedWriter, FileWriter, Writer}
import java.nio.file.{Files, Paths}

import com.yahoo.ycsb.{ByteIterator, Status}
import org.apache.logging.log4j.scala.Logging
import java.util
import java.util.Base64

import scala.util.Random
import scala.collection.JavaConverters._



class MockDB extends JoveoDBBatch with  Logging {
  override type BatchKey = String

  private var db : Writer = _

  private val id = Random.alphanumeric.take(10).mkString("")

  override def cleanup(): Unit = {
    super.cleanup()
    db.flush()
    db.close()
  }

  override def init(): Unit = {
    super.init()
    val p = getProperties
    val root = Paths.get(  p.getProperty("db.mock_path", "/tmp/scylla_mock/") )
    if( ! root.toFile.exists() )
      Files.createDirectories( root )
    val dbPath = root.resolve( id+ ".txt"  )
    db = new BufferedWriter(new FileWriter( dbPath.toFile))
  }

  private def serialize( result: util.Map[String, ByteIterator] ): String = {
    result.asScala.map {
      case ( key, value ) =>
        val valueStr = value.asInstanceOf[JvByteIterator] match {
          case JVBoolean(underlying) =>  underlying.toString
          case JVByte(underlying) => underlying.toString
          case JVShort(underlying) => underlying.toString
          case JVInt(underlying) => underlying.toString
          case JVLong(underlying) => underlying.toString
          case JVFloat(underlying) => underlying.toString
          case JVDouble(underlying) => underlying.toString
          case JVText(underlying) => underlying
          case JVBlob(underlying) => s" [base-64 size=${underlying.length}] content="+Base64.getEncoder.encodeToString( underlying )
          case JVDate(underlying) => underlying.toString
          case JVTimestamp(underlying) => underlying.toString
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

  override protected def getKey(op: DBOperation, key: String, operation: YCSBOperation): BatchKey = id

  override protected def bulkRead(op: YCSBOperation)(ids: List[String]): Status = {
    log( s"op=READ, keys=${ids.mkString("::")}, fields=${op.fields.mkString("::")},null\n")
  }

  override protected def bulkWrite(op: YCSBOperation)(entities: List[ Entity ]): Status = {
    val keys = entities.map(_._1).mkString("::")
    val fields = op.fields.mkString("::")
    val elems = entities.map(_._2).map( serialize ).mkString("\n")
    log( s"op=READ, keys=$keys, fields=$fields,elems=\n$elems\n")
  }
}
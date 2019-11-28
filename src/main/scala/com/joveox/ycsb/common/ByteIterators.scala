package com.joveox.ycsb.common

import java.time.Instant
import java.util.Date

import com.yahoo.ycsb.ByteIterator
import upack._

import DataType._

import scala.collection.mutable

sealed trait JvByteIterator extends ByteIterator{
  val `type`: DataType
  val underlying: Any

  lazy val mat = encoded
  lazy val it = mat.toIterator
  private var nextIdx = 0

  override def hasNext: Boolean = it.hasNext

  override def nextByte(): Byte = {
    nextIdx = nextIdx + 1
    it.next()
  }

  override def bytesLeft(): Long = length - nextIdx

  lazy val length = mat.length

  def encoded: Array[Byte] = JvByteIterator.encode( this )

  override def toString: String = underlying.toString
}

case class JVBoolean(underlying: Boolean) extends JvByteIterator {
  override val `type` = BOOLEAN
}

case class JVByte(underlying: Byte) extends JvByteIterator {
  override val `type` = BYTE
}

case class JVShort(underlying: Short) extends JvByteIterator {
  override val `type` = SHORT
}

case class JVInt(underlying: Int) extends JvByteIterator {
  override val `type` = INT
}

case class JVLong(underlying: Long) extends JvByteIterator {
  override val `type` = LONG
}

case class JVFloat(underlying: Float) extends JvByteIterator {
  override val `type` = FLOAT
}

case class JVDouble(underlying: Double) extends JvByteIterator {
  override val `type` = DOUBLE
}

case class JVText(underlying: String) extends JvByteIterator {
  override val `type` = TEXT
}

case class JVBlob(underlying: Array[Byte]) extends JvByteIterator {
  override val `type` = BLOB
}

case class JVDate(underlying: Date) extends JvByteIterator {
  override val `type` = DATE
}

case class JVTimestamp(underlying: Instant) extends JvByteIterator {
  override val `type` = TIMESTAMP
}

case class JVNull(override val `type`: DataType) extends JvByteIterator {
  override val underlying = None
}

case class JVList(underlying: List[JvByteIterator]) extends JvByteIterator {
  override val `type` = LIST
}

case class JVMap(underlying: Map[ JvByteIterator, JvByteIterator] ) extends JvByteIterator {
  override val `type` = MAP
}


object JvByteIterator {

  private val typesEncoded: Map[ DataType, Byte ] = Map(
    NOTHING -> 0,
    BOOLEAN -> 1,
    BYTE -> 2,
    SHORT -> 3,
    INT -> 4,
    LONG -> 5,
    FLOAT -> 6,
    DOUBLE -> 7,
    TEXT -> 8,
    BLOB -> 9,
    DATE -> 10,
    TIMESTAMP -> 11,
    LIST -> 12,
    MAP -> 13
  )
  private val typesDecoded : Array[ DataType ] = Array(
    NOTHING,
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    TEXT,
    BLOB,
    DATE,
    TIMESTAMP,
    LIST,
    MAP
  )

  def encode(t: DataType): Byte = {
    typesEncoded( t )
  }

  def decode(b: Byte): DataType = {
    typesDecoded( b )
  }

  def encode(v: JvByteIterator): Array[Byte] = {
    upack.write( pack( v ) )
  }

  def pack( elem: JvByteIterator ): upack.Msg = {
    elem match {
      case JVBoolean(underlying) => upack.Bool( underlying )
      case JVByte(underlying) => upack.Ext( encode( BYTE ), upack.write( upack.Int32( underlying ) ) )
      case JVShort(underlying) => upack.Ext( encode( SHORT ), upack.write( upack.Int32( underlying ) ) )
      case JVInt(underlying) => upack.Int32( underlying )
      case JVLong(underlying) => upack.Int64( underlying )
      case JVFloat(underlying) => upack.Float32( underlying )
      case JVDouble(underlying) => upack.Float64( underlying )
      case JVText(underlying) => upack.Str( underlying )
      case JVBlob(underlying) => upack.Binary( underlying )
      case JVDate(underlying) => upack.Int64( underlying.getTime )
      case JVTimestamp(underlying) => upack.Int64( underlying.getEpochSecond )
      case JVNull( dataType ) => upack.Ext( encode( dataType ), Array.empty )
      case JVList(underlying) => upack.Arr( underlying.map(value => pack( value ) ):_* )
      case JVMap(underlying) =>
        val map = new mutable.LinkedHashMap[ upack.Msg, upack.Msg ]()
        for (i <- underlying) map.put( pack(i._1), pack(i._2) )
        upack.Obj(map)
    }
  }

  def unpack( content: Msg ): JvByteIterator = {
    content match {
      case Null => JVNull( NOTHING )
      case Int32(value) => JVInt( value )
      case Int64(value) => JVLong( value )
      case UInt64(value) => JVLong( value )
      case Float32(value) => JVFloat( value )
      case Float64(value) => JVDouble( value )
      case Str(value) => JVText( value )
      case Binary(value) => JVBlob( value )
      case Arr(value) => JVList( value.map( unpack ).toList )
      case Obj(value) => JVMap( value.map( kv => unpack( kv._1 ) -> unpack( kv._2 ) ).toMap )
      case Ext(tag, data) =>
        decode(tag) match {
          case BYTE => JVByte( upack.read( data ).asInstanceOf[ Int32 ].value.toByte  )
          case SHORT =>
            JVShort( upack.read( data ).asInstanceOf[ Int32 ].value.toShort )
          case dataType => JVNull( dataType )
        }
      case upack.True => JVBoolean( true )
      case upack.False => JVBoolean( false )
    }
  }

  def decode(bytes: Array[Byte]): JvByteIterator = {
    unpack( upack.read( bytes ) )
  }
}

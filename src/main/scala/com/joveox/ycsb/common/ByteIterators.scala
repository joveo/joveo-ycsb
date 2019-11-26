package com.joveox.ycsb.common

import java.time.Instant
import java.util.Date

import com.yahoo.ycsb.ByteIterator

sealed trait JvByteIterator[ T ] extends ByteIterator{

  val underlying : T

  override def hasNext: Boolean = ???

  override def nextByte(): Byte = ???

  override def bytesLeft(): Long = ???

  override def toString: String = underlying.toString

  val `type` : DataType[ T ]
}

case class JVBoolean( underlying: Boolean ) extends JvByteIterator[ Boolean ]{
  override val `type` = BOOLEAN
}
case class JVByte( underlying: Byte ) extends JvByteIterator[ Byte ]{
  override val `type` = BYTE
}
case class JVShort( underlying: Short ) extends JvByteIterator[ Short ]{
  override val `type` = SHORT
}
case class JVInt( underlying: Int ) extends JvByteIterator[ Int ]{
  override val `type` = INT
}
case class JVLong( underlying: Long ) extends JvByteIterator[ Long ]{
  override val `type` = LONG
}
case class JVFloat( underlying: Float ) extends JvByteIterator[ Float ]{
  override val `type` = FLOAT
}
case class JVDouble( underlying: Double ) extends JvByteIterator[ Double ]{
  override val `type` = DOUBLE
}
case class JVText( underlying: String ) extends JvByteIterator[ String ]{
  override val `type` = TEXT
}
case class JVBlob( underlying: Array[ Byte ] ) extends JvByteIterator[ Array[ Byte ] ]{
  override val `type` = BLOB
}
case class JVDate( underlying: Date ) extends JvByteIterator[ Date ]{
  override val `type` = DATE
}
case class JVTimestamp( underlying: Instant ) extends JvByteIterator[ Instant ]{
  override val `type` = TIMESTAMP
}
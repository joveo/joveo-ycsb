package com.joveox.ycsb.common

import java.time.Instant
import java.util.Date

import com.yahoo.ycsb.ByteIterator

trait JvByteIterator extends ByteIterator{
  override def hasNext: Boolean = ???

  override def nextByte(): Byte = ???

  override def bytesLeft(): Long = ???
}

case class JVBoolean( underlying: Boolean ) extends JvByteIterator
case class JVByte( underlying: Byte ) extends JvByteIterator
case class JVShort( underlying: Short ) extends JvByteIterator
case class JVInt( underlying: Int ) extends JvByteIterator
case class JVLong( underlying: Long ) extends JvByteIterator
case class JVFloat( underlying: Float ) extends JvByteIterator
case class JVDouble( underlying: Double ) extends JvByteIterator
case class JVText( underlying: String ) extends JvByteIterator
case class JVBlob( underlying: Array[ Byte ] ) extends JvByteIterator
case class JVDate( underlying: Date ) extends JvByteIterator
case class JVTimestamp( underlying: Instant ) extends JvByteIterator
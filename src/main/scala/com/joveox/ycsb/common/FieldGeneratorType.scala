package com.joveox.ycsb.common

import java.time.Instant
import java.util.{Base64, Date}

import scala.util.Random

sealed trait FieldGenerator[ T ] {
  val dataType: DataType[T]
  def init( seed: SeedData ): Unit
  def next( threadId: Int, count: Int ): T
  def nextByteIterator( threadId: Int, count: Int ): JvByteIterator[ T ] = dataType.asByteIterator( next( threadId, count ) )
}

sealed trait RandomFieldGenerator[ T ] extends FieldGenerator[ T ]{
  def init( seed: SeedData ): Unit = ()
}

case object BooleanGenerator extends RandomFieldGenerator[ Boolean ] {
  val dataType = BOOLEAN
  override def next(threadId: Int, count: Int): Boolean = Random.nextBoolean()
}
case object ByteGenerator extends RandomFieldGenerator[ Byte ] {
  val dataType = BYTE
  override def next(threadId: Int, count: Int): Byte = Random.nextInt( Byte.MaxValue ).toByte
}
case object ShortGenerator extends RandomFieldGenerator[ Short ] {
  val dataType = SHORT
  override def next(threadId: Int, count: Int): Short = Random.nextInt( Short.MaxValue ).toShort
}
case object IntGenerator extends RandomFieldGenerator[ Int ] {
  val dataType = INT
  override def next(threadId: Int, count: Int): Int = Random.nextInt()
}
case object LongGenerator extends RandomFieldGenerator[ Long ] {
  val dataType = LONG
  override def next(threadId: Int, count: Int): Long = Random.nextLong()
}
case object FloatGenerator extends RandomFieldGenerator[ Float ] {
  val dataType = FLOAT
  override def next(threadId: Int, count: Int): Float = Random.nextFloat()
}
case object DoubleGenerator extends RandomFieldGenerator[ Double ] {
  val dataType = DOUBLE
  override def next(threadId: Int, count: Int): Double = Random.nextDouble()
}
case object TextGenerator extends RandomFieldGenerator[ String ] {
  val dataType = TEXT
  override def next(threadId: Int, count: Int): String = Random.alphanumeric.take( 100 ).mkString("")
}
case object BlobGenerator extends RandomFieldGenerator[ Array[ Byte ] ] {
  val dataType = BLOB
  override def next(threadId: Int, count: Int): Array[ Byte ] = {
    val bytes = new Array[ Byte ]( 100 )
    Random.nextBytes( bytes )
    bytes
  }
}
case object DateGenerator extends RandomFieldGenerator[ Date ] {
  val dataType = DATE
  override def next(threadId: Int, count: Int): Date =
    new Date( 1546304461000L + Random.nextInt( 1000) * 86400000L )
}
case object TimestampGenerator extends RandomFieldGenerator[ Instant ] {
  val dataType = TIMESTAMP
  override def next(threadId: Int, count: Int): Instant =
    Instant.ofEpochMilli( 1546304461000L + Random.nextInt( 1000) * 86400000L )
}

sealed trait SeedGenerator[ T ] extends FieldGenerator[ T ]{
  val id: String
  val random: Boolean
  private var data = Array.empty[ String ]
  private var currentIdx = -1
  def init( seed: SeedData ): Unit = {
    data = seed.getSeedData( id )
  }
  protected def value( content: String ): T
  override def next(threadId: Int, count: Int): T = {
    assert( ! data.isEmpty, s" Seed generator $id is empty. ")
    val content = if( random ) data( Random.nextInt( data.length ) )
    else{
      currentIdx = ( currentIdx + 1 ) % data.length
      data( currentIdx )
    }
    value( content )
  }
}

case class SeedTextGenerator( random: Boolean, id: String ) extends SeedGenerator[ String ]{
  override val dataType = TEXT
  override protected def value(content: String): String = content
}
case class SeedBlobGenerator( random: Boolean, id: String ) extends SeedGenerator[ Array[ Byte ] ]{
  override val dataType = BLOB
  override protected def value(content: String): Array[ Byte ] = Base64.getDecoder.decode( content )
}

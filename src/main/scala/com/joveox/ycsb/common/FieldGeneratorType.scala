package com.joveox.ycsb.common

import java.time.Instant
import java.util.{Base64, Date}

import DataType._

import scala.util.Random

sealed trait FieldGenerator {
  val dataType: DataType
  def init( seed: SeedData ): Unit
  def next( threadId: Int, count: Int ): JvByteIterator
}

sealed trait RandomFieldGenerator extends FieldGenerator{
  def init( seed: SeedData ): Unit = ()
}

case object BooleanGenerator extends RandomFieldGenerator {
  val dataType = BOOLEAN
  override def next(threadId: Int, count: Int): JVBoolean = JVBoolean( Random.nextBoolean() )
}
case object ByteGenerator extends RandomFieldGenerator{
  val dataType = BYTE
  override def next(threadId: Int, count: Int): JVByte = JVByte(Random.nextInt( Byte.MaxValue ).toByte)
}
case object ShortGenerator extends RandomFieldGenerator{
  val dataType = SHORT
  override def next(threadId: Int, count: Int): JVShort = JVShort( Random.nextInt( Short.MaxValue ).toShort )
}
case object IntGenerator extends RandomFieldGenerator{
  val dataType = INT
  override def next(threadId: Int, count: Int): JVInt = JVInt( Random.nextInt() )
}
case object LongGenerator extends RandomFieldGenerator{
  val dataType = LONG
  override def next(threadId: Int, count: Int): JVLong = JVLong( Random.nextLong() )
}
case object FloatGenerator extends RandomFieldGenerator {
  val dataType = FLOAT
  override def next(threadId: Int, count: Int): JVFloat = JVFloat( Random.nextFloat() )
}
case object DoubleGenerator extends RandomFieldGenerator{
  val dataType = DOUBLE
  override def next(threadId: Int, count: Int): JVDouble = JVDouble( Random.nextDouble() )
}
case class TextGenerator( minSize: Int = 0, maxSize: Int = 16 ) extends RandomFieldGenerator{
  val dataType = TEXT
  override def next(threadId: Int, count: Int): JVText = {
    val size = Math.max( 0, minSize ) + Random.nextInt( Math.max( 0, maxSize))
    JVText( Random.alphanumeric.take( size ).mkString("") )
  }
}
case class BlobGenerator( minSize: Int = 0, maxSize: Int = 16 ) extends RandomFieldGenerator{
  val dataType = BLOB
  override def next(threadId: Int, count: Int): JVBlob = {
    val size = Math.max( 0, minSize ) + Random.nextInt( Math.max( 0, maxSize))
    val bytes = new Array[ Byte ]( size )
    Random.nextBytes( bytes )
    JVBlob(bytes)
  }
}
case object DateGenerator extends RandomFieldGenerator {
  val dataType = DATE
  override def next(threadId: Int, count: Int): JVDate = JVDate(
    new Date( 1546304461000L + Random.nextInt( 1000) * 86400000L )
  )
}
case object TimestampGenerator extends RandomFieldGenerator {
  val dataType = TIMESTAMP
  override def next(threadId: Int, count: Int): JVTimestamp =
    JVTimestamp ( Instant.ofEpochMilli( 1546304461000L + Random.nextInt( 1000) * 86400000L ) )
}

sealed trait SeedGenerator extends FieldGenerator{
  val id: String
  val random: Boolean
  private var data = Array.empty[ String ]
  private var currentIdx = -1
  def init( seed: SeedData ): Unit = {
    data = seed.getSeedData( id )
  }
  protected def value( content: String ): JvByteIterator
  override def next(threadId: Int, count: Int): JvByteIterator = {
    assert( ! data.isEmpty, s" Seed generator $id is empty. ")
    val content = if( random ) data( Random.nextInt( data.length ) )
    else{
      currentIdx = ( currentIdx + 1 ) % data.length
      data( currentIdx )
    }
    value( content )
  }
}

case class SeedTextGenerator( id: String, random: Boolean = false ) extends SeedGenerator {
  override val dataType = TEXT
  override protected def value(content: String): JVText = JVText( content )
}
case class SeedBlobGenerator( id: String, random: Boolean = false  ) extends SeedGenerator{
  override val dataType = BLOB
  override protected def value(content: String): JVBlob = JVBlob( Base64.getDecoder.decode( content ) )
}


case class ListGenerator( inner: FieldGenerator, minSize: Int, maxSize: Int ) extends FieldGenerator {
  val dataType = LIST

  override def init(seed: SeedData): Unit = {
    inner.init( seed )
  }

  override def next(threadId: Int, count: Int): JVList = {
    val size = Math.max( 0, minSize ) + Random.nextInt( Math.max( 0, maxSize))
    JVList(
      (0 until size).toList.map{ i =>
        inner.next( threadId, count )
      }
    )
  }

}

case class MapGenerator( schema: Map[ String, FieldGenerator ] ) extends FieldGenerator {
  val dataType = MAP
  override def init(seed: SeedData): Unit = {
    schema.valuesIterator.foreach( _.init( seed ) )
  }
  override def next(threadId: Int, count: Int): JVMap = {
    JVMap(
      schema.map{
        case ( field, generator ) =>
          JVText( field ) -> generator.next( threadId, count )
      }.toMap
    )
  }

}


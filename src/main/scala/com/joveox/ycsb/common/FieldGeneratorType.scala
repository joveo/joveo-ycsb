package com.joveox.ycsb.common

import java.time.Instant
import java.util
import java.util.{Base64, Date}

import com.yahoo.ycsb.ByteIterator
import enumeratum._

import scala.util.Random
import scala.collection.JavaConverters._

sealed trait FieldGeneratorType extends EnumEntry
object FieldGeneratorType extends Enum[FieldGeneratorType] {

  val values = findValues

  case object RANDOM extends FieldGeneratorType

  case object SEED_RANDOM extends FieldGeneratorType

  case object SEED_SEQ extends FieldGeneratorType

}

trait FieldGenerator {
  def next( `type`: FieldType ): ByteIterator
}

abstract class SeedFieldGenerator( data: Array[ String ] ) extends FieldGenerator {

  def nextIdx(): Int

  override def next(`type`: FieldType): ByteIterator = {
    val idx = nextIdx()
    `type` match {
      case FieldType.TEXT => JVText( data( idx ) )
      case FieldType.BLOB =>
        val content = Base64.getDecoder.decode( data( idx ) )
        JVBlob( content )
      case _ => throw new IllegalArgumentException( " Only TEXT and BLOB types are supported for SeedGenerator")
    }
  }
}

case class RandomFieldGenerator() extends FieldGenerator {
  override def next(`type`: FieldType ): ByteIterator = {
    `type` match {
      case FieldType.BOOLEAN => JVBoolean( Random.nextBoolean )
      case FieldType.BYTE => JVByte( Random.nextInt( Byte.MaxValue ).toByte )
      case FieldType.SHORT => JVShort( Random.nextInt( Short.MaxValue ).toShort )
      case FieldType.INT => JVInt( Random.nextInt )
      case FieldType.LONG => JVLong( Random.nextLong )
      case FieldType.FLOAT => JVFloat( Random.nextFloat )
      case FieldType.DOUBLE => JVDouble( Random.nextDouble )
      case FieldType.TEXT => JVText( Random.alphanumeric.take( 100 ).mkString("") )
      case FieldType.BLOB =>
        val bytes = new Array[ Byte ]( 100 )
        Random.nextBytes( bytes )
        JVBlob( bytes )
      case FieldType.DATE => JVDate( new Date( 1546304461000L + Random.nextInt( 1000) * 86400000L ) )
      case FieldType.TIMESTAMP => JVTimestamp( Instant.ofEpochMilli( 1546304461000L + Random.nextInt( 1000) * 86400000L ) )

    }
  }
}

case class SeqSeedFieldGenerator( data: Array[ String ] ) extends SeedFieldGenerator( data ) {
  private var currentIdx = 0

  override def nextIdx(): Int = {
    currentIdx = ( currentIdx + 1 ) % data.length
    currentIdx
  }
}

case class RandomSeedFieldGenerator( data: Array[ String ] ) extends SeedFieldGenerator( data ) {

  override def nextIdx(): Int = {
    Random.nextInt( data.length )
  }
}

class RecordGenerator( schema: Schema ){

  private val primaryKey = schema.primaryKey
  private val fieldsByName = schema.fields.groupBy( _.name ).map( kv => kv._1 -> kv._2.head )
  private var generators = Map.empty[Field, FieldGenerator]

  private def buildGenerators(): Map[Field, FieldGenerator] = {
    val generators: Map[Field, FieldGenerator] = schema.fields.map{ field =>
      val generator = field.generator match {
        case FieldGeneratorType.RANDOM => RandomFieldGenerator()
        case FieldGeneratorType.SEED_RANDOM => RandomSeedFieldGenerator( schema.getSeedData( field.name ) )
        case FieldGeneratorType.SEED_SEQ => SeqSeedFieldGenerator(  schema.getSeedData( field.name )  )
      }
      field -> generator
    }.toMap
    generators
  }

  def init(): Unit = {
    generators = buildGenerators()
  }

  def nextKey(  threadId: Int, idx: Int ): String = {
    val content = generators ( primaryKey ).next( FieldType.TEXT ).asInstanceOf[ JVText ].underlying
    s"$threadId--$idx--content"
  }

  def nextFields( fieldNames: String * ): util.Map[String, ByteIterator] = {
    fieldNames.filterNot( _ == primaryKey.name ).map{ name =>
      val field = fieldsByName( name )
      val generator = generators( field )
      name -> generator.next( field.`type` )
    }.toMap.asJava
  }

  def nextAll( fieldNames: String * ): util.Map[ String, ByteIterator ] = {
    nextFields( schema.fields.map( _.name ):_* )
  }


}

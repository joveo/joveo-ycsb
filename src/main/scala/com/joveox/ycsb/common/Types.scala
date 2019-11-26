package com.joveox.ycsb.common

import java.time.Instant
import java.util.Date

sealed trait DataType[ T ] {
  def asByteIterator(value: T ): JvByteIterator[ T ]
  def asField( name: String ): Field[ T ]
}

case object BOOLEAN extends DataType[ Boolean ]{
  override def asByteIterator(value: Boolean): JVBoolean = JVBoolean( value )
  override def asField(name: String): BOOLEANField = BOOLEANField( name )
}
case object BYTE extends DataType[ Byte ] {
  override def asByteIterator(value: Byte): JVByte = JVByte( value )
  override def asField(name: String): BYTEField = BYTEField( name )
}
case object SHORT extends DataType[ Short ] {
  override def asByteIterator(value: Short): JVShort = JVShort( value )
  override def asField(name: String): SHORTField = SHORTField( name )
}
case object INT extends DataType[ Int ] {
  override def asByteIterator(value: Int): JVInt = JVInt( value )
  override def asField(name: String): INTField = INTField( name )
}
case object LONG extends DataType[ Long ] {
  override def asByteIterator(value: Long): JVLong = JVLong( value )
  override def asField(name: String): LONGField = LONGField( name )
}
case object FLOAT extends DataType[ Float ] {
  override def asByteIterator(value: Float): JVFloat = JVFloat( value )
  override def asField(name: String): FLOATField = FLOATField( name )
}
case object DOUBLE extends DataType[ Double ] {
  override def asByteIterator(value: Double): JVDouble = JVDouble( value )
  override def asField(name: String): DOUBLEField = DOUBLEField( name )
}
case object TEXT extends DataType[ String ] {
  override def asByteIterator(value: String): JVText = JVText( value )
  override def asField(name: String): TEXTField = TEXTField( name )
}
case object BLOB extends DataType[ Array[ Byte ] ] {
  override def asByteIterator(value: Array[Byte]): JVBlob = JVBlob( value )
  override def asField(name: String): BLOBField = BLOBField( name )
}
case object DATE extends DataType[ Date ] {
  override def asByteIterator(value: Date): JVDate = JVDate( value )
  override def asField(name: String): DATEField = DATEField( name )
}
case object TIMESTAMP extends DataType[ Instant ] {
  override def asByteIterator(value: Instant): JVTimestamp = JVTimestamp( value )
  override def asField(name: String): TIMESTAMPField = TIMESTAMPField( name )
}

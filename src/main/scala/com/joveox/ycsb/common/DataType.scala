package com.joveox.ycsb.common

import enumeratum.{Enum, EnumEntry}



sealed trait DataType extends EnumEntry
object DataType extends Enum[DataType] {
  val values = findValues
  case object NOTHING extends DataType
  case object BOOLEAN extends DataType
  case object BYTE extends DataType
  case object SHORT extends DataType
  case object INT extends DataType
  case object LONG extends DataType
  case object FLOAT extends DataType
  case object DOUBLE extends DataType
  case object TEXT extends DataType
  case object BLOB extends DataType
  case object DATE extends DataType
  case object TIMESTAMP extends DataType
  case object LIST extends DataType
  case object MAP extends DataType
}

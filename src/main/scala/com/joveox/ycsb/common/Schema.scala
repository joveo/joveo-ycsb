package com.joveox.ycsb.common

import java.time.Instant
import java.util.Date

sealed trait Field[ T ]{
  val name: String
  val `type`: DataType[ T ]
}

case class BOOLEANField(override val name: String) extends Field[ Boolean ]{
  override val `type` = BOOLEAN
}
case class BYTEField(override val name: String) extends Field[ Byte ]{
  override val `type` = BYTE
}
case class SHORTField(override val name: String) extends Field[ Short ]{
  override val `type` = SHORT
}
case class INTField(override val name: String) extends Field[ Int ]{
  override val `type` = INT
}
case class LONGField(override val name: String) extends Field[ Long ]{
  override val `type` = LONG
}
case class FLOATField(override val name: String) extends Field[ Float ]{
  override val `type` = FLOAT
}
case class DOUBLEField(override val name: String) extends Field[ Double ]{
  override val `type` = DOUBLE
}
case class TEXTField(override val name: String) extends Field[ String ]{
  override val `type` = TEXT
}
case class BLOBField(override val name: String) extends Field[ Array[ Byte ] ]{
  override val `type` = BLOB
}
case class DATEField(override val name: String) extends Field[ Date ]{
  override val `type` = DATE
}
case class TIMESTAMPField(override val name: String) extends Field[ Instant ]{
  override val `type` = TIMESTAMP
}


case class Schema(
                   db: String,
                   name: String,
                   primaryKey: TEXTField,
                   fields: List[ Field[ _ ] ] = List.empty
                 ) {
  val allFields: List[Field[_]] = primaryKey :: fields
}
